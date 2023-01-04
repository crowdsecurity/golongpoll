package golongpoll

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	//"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/gofrs/uuid"
)

const (
	// forever is a magic number to represent 'Forever' in
	// LongpollOptions.EventTimeToLiveSeconds
	forever = -1001
)

// LongpollManager is used to interact with the internal longpolling pup-sub
// goroutine that is launched via StartLongpoll(Options).
//
// LongpollManager.SubscriptionHandler can be served directly or wrapped in an
// Http handler to add custom behavior like authentication. Events can be
// published via LongpollManager.Publish(). The manager can be stopped via
// Shutdown() or ShutdownWithTimeout(seconds int).
//
// Events can also be published by http clients via LongpollManager.PublishHandler
// if desired. Simply serve this handler directly, or wrapped in an Http handler
// that adds desired authentication/access controls.
//
// If for some reason you want multiple goroutines handling different pub-sub
// channels, you can simply create multiple LongpollManagers and serve their
// subscription handlers on separate URLs.
type LongpollManager struct {
	subManager    *subscriptionManager
	eventsIn      chan<- *Event
	eventsDeleted chan<- uuid.UUID
	stopSignal    chan<- bool
	// SubscriptionHandler is an Http handler function that can be served
	// directly or wrapped within another handler function that adds additional
	// behavior like authentication or business logic.
	SubscriptionHandler func(w http.ResponseWriter, r *http.Request)
	// flag whether or not StartLongpoll has been called
	started bool
	// flag whether or not LongpollManager.Shutdown has been called--enforces
	// use-only-once.
	stopped bool
	Logger  logrus.FieldLogger
}

// Publish an event for a given subscription category.  This event can have any
// arbitrary data that is convert-able to JSON via the standard's json.Marshal()
// the category param must be a non-empty string no longer than 1024,
// otherwise you get an error. Cannot be called after LongpollManager.Shutdown()
// or LongpollManager.ShutdownWithTimeout(seconds int).
func (m *LongpollManager) Publish(event *Event) error {
	if !m.started {
		panic("LongpollManager cannot call Publish, never started. LongpollManager must be created via StartLongPoll(Options).")
	}

	if m.stopped {
		panic("LongpollManager cannot call Publish, already stopped.")
	}

	if len(event.Category) == 0 {
		return errors.New("empty category")
	}
	if len(event.Category) > 1024 {
		return errors.New("category cannot be longer than 1024")
	}

	if event.ID == uuid.Nil {
		return errors.New("event UUID cannot be empty")
	}

	EventsInQueue.With(prometheus.Labels{"category": event.Category}).Inc()

	m.eventsIn <- event
	return nil
}

func (m *LongpollManager) Unpublish(id uuid.UUID) error {
	m.eventsDeleted <- id
	return nil
}

// Shutdown will stop the LongpollManager's run goroutine and call Addon.OnShutdown.
// This will block on the Addon's shutdown call if an AddOn is provided.
// In addition to allowing a graceful shutdown, this can be useful if you want
// to turn off longpolling without terminating your program.
// After a shutdown, you can't call Publish() or get any new results from the
// SubscriptionHandler. Multiple calls to this function on the same manager will
// result in a panic.
func (m *LongpollManager) Shutdown() {
	if m.stopped {
		panic("LongpollManager cannot be stopped more than once.")
	}

	if !m.started {
		panic("LongpollManager cannot be stopped, never started. LongpollManager must be created via StartLongPoll(Options).")
	}

	m.stopped = true
	close(m.stopSignal)
	<-m.subManager.shutdownDone
}

// ShutdownWithTimeout will call Shutdown but only block for a provided
// amount of time when waiting for the shutdown to complete.
// Returns an error on timeout, otherwise nil. This can only be called once
// otherwise it will panic.
func (m *LongpollManager) ShutdownWithTimeout(seconds int) error {
	if m.stopped {
		panic("LongpollManager cannot be stopped more than once.")
	}

	if !m.started {
		panic("LongpollManager cannot be stopped, never started. LongpollManager must be created via StartLongPoll(Options).")
	}

	m.stopped = true
	close(m.stopSignal)
	select {
	case <-m.subManager.shutdownDone:
		return nil
	case <-time.After(time.Duration(seconds) * time.Second):
		return errors.New("LongpollManager Shutdown timeout exceeded.")
	}
}

// Options for LongpollManager that get sent to StartLongpoll(options)
type Options struct {
	Logger logrus.FieldLogger

	// Max client timeout seconds to be accepted by the SubscriptionHandler
	// (The 'timeout' HTTP query param).  Defaults to 110.
	// NOTE: if serving behind a proxy/webserver, make sure the max allowed
	// timeout here is less than that server's configured HTTP timeout!
	// Typically, servers will have a 60 or 120 second timeout by default.
	MaxLongpollTimeoutSeconds int

	// How many events to buffer per subscriptoin category before discarding
	// oldest events due to buffer being exhausted.  Larger buffer sizes are
	// useful for high volumes of events in the same categories.  But for
	// low-volumes, smaller buffer sizes are more efficient.  Defaults to 250.
	MaxEventBufferSize int

	// How long (seconds) events remain in their respective category's
	// eventBuffer before being deleted. Deletes old events even if buffer has
	// the room.  Useful to save space if you don't need old events.
	// You can use a large MaxEventBufferSize to handle spikes in event volumes
	// in a single category but have a relatively short EventTimeToLiveSeconds
	// value to save space in the more common low-volume case.
	// Defaults to infinite/forever TTL.
	EventTimeToLiveSeconds int

	// Whether or not to delete an event as soon as it is retrieved via an
	// HTTP longpoll.  Saves on space if clients only interested in seeing an
	// event once and never again.  Meant mostly for scenarios where events
	// act as a sort of notification and each subscription category is assigned
	// to a single client.  As soon as any client(s) pull down this event, it's
	// gone forever.  Notice how multiple clients can get the event if there
	// are multiple clients actively in the middle of a longpoll when a new
	// event occurs.  This event gets sent to all listening clients and then
	// the event skips being placed in a buffer and is gone forever.
	DeleteEventAfterFirstRetrieval bool

	// Optional add-on to add behavior like event persistence to longpolling.
	AddOn AddOn
}

// StartLongpoll creates a LongpollManager, starts the internal pub-sub goroutine
// and returns the manager reference which you can use anywhere to Publish() events
// or attach a URL to the manager's SubscriptionHandler member.  This function
// takes an Options struct that configures the longpoll behavior.
// If Options.EventTimeToLiveSeconds is omitted, the default is forever.
func StartLongpoll(opts Options) (*LongpollManager, error) {
	// default if not specified (likely struct skipped defining this field)
	if opts.MaxLongpollTimeoutSeconds == 0 {
		opts.MaxLongpollTimeoutSeconds = 110
	}
	// default if not specified (likely struct skipped defining this field)
	if opts.MaxEventBufferSize == 0 {
		opts.MaxEventBufferSize = 250
	}
	// If TTL is zero, default to FOREVER
	if opts.EventTimeToLiveSeconds == 0 {
		opts.EventTimeToLiveSeconds = forever
	}
	if opts.MaxEventBufferSize < 1 {
		return nil, errors.New("Options.MaxEventBufferSize must be at least 1")
	}
	if opts.MaxLongpollTimeoutSeconds < 1 {
		return nil, errors.New("Options.MaxLongpollTimeoutSeconds must be at least 1")
	}
	// TTL must be positive, non-zero, or the magic forever value (a negative const)
	if opts.EventTimeToLiveSeconds < 1 && opts.EventTimeToLiveSeconds != forever {
		return nil, errors.New("options.EventTimeToLiveSeconds must be at least 1 or the constant longpoll.FOREVER")
	}

	if opts.Logger == nil {
		opts.Logger = logrus.New().WithField("component", "longpoll")
	}

	prometheus.MustRegister(EventsInQueue)
	channelSize := 100
	clientRequestChan := make(chan *clientSubscription, channelSize)
	clientTimeoutChan := make(chan *clientCategoryPair, channelSize)
	events := make(chan *Event, channelSize)
	deletedEvents := make(chan uuid.UUID, channelSize)
	// never has a send, only a close, so no larger capacity needed:
	quit := make(chan bool, 1)
	subManager := subscriptionManager{
		clientSubscriptions:            clientRequestChan,
		ClientTimeouts:                 clientTimeoutChan,
		Events:                         events,
		DeletedEvents:                  deletedEvents,
		ClientSubChannels:              make(map[string]map[uuid.UUID]chan<- []*Event),
		SubEventBuffer:                 make(map[string]*expiringBuffer),
		Quit:                           quit,
		shutdownDone:                   make(chan bool, 1),
		MaxLongpollTimeoutSeconds:      opts.MaxLongpollTimeoutSeconds,
		MaxEventBufferSize:             opts.MaxEventBufferSize,
		EventTimeToLiveSeconds:         opts.EventTimeToLiveSeconds,
		DeleteEventAfterFirstRetrieval: opts.DeleteEventAfterFirstRetrieval,
		// check for stale categories every 3 minutes.
		// remember we do expiration/cleanup on individual buffers whenever
		// activity occurs on that buffer's category (client request, event published)
		// so this periodic purge check is only needed to remove events on
		// categories that have been inactive for a while.
		staleCategoryPurgePeriodSeconds: 60 * 3,
		// set last purge time to present so we wait a full period before puring
		// if this defaulted to zero then we'd immediately do a purge which is unnecessary
		lastStaleCategoryPurgeTime: timeToEpochMilliseconds(time.Now()),
		// A priority queue (min heap) that keeps track of event buffers by their
		// last event time.  Used to know when to delete inactive categories/buffers.
		bufferPriorityQueue: make(priorityQueue, 0),
		AddOn:               opts.AddOn,
		Logger:              opts.Logger.WithField("component", "subscription_manager"),
	}
	heap.Init(&subManager.bufferPriorityQueue)

	// Optionally prepopulate with data
	if subManager.AddOn != nil {
		startChan := subManager.AddOn.OnLongpollStart()

		// Don't add events if they would already be considered expired by the longpoll options.
		cutoffTime := int64(0)
		if subManager.EventTimeToLiveSeconds > 0 {
			cutoffTime = timeToEpochMilliseconds(time.Now()) - int64(subManager.EventTimeToLiveSeconds*1000)
		}

		currentEventTime := int64(0)
		for {
			event, ok := <-startChan
			if ok {
				if event.Timestamp < currentEventTime {
					// The internal datastructures assume data is added in chonological order.
					// Otherwise, lots of code would have to pay the penalty of ensuring
					// data is ordered and support insert-then-sort which I'm not prepared to
					// support.
					panic("Events supplied via AddOn.OnLongpollStart must be in chronological order (oldest first).")
				}

				currentEventTime = event.Timestamp

				if event.Timestamp > cutoffTime {
					subManager.handleNewEvent(event)
				}
			} else {
				// channel closed, we're done populating
				break
			}
		}
		// do any cleanup if options dictate it
		subManager.purgeStaleCategories()
	}

	// Start subscription manager
	go subManager.run()
	lpManager := LongpollManager{
		subManager:    &subManager,
		eventsIn:      events,
		eventsDeleted: deletedEvents,
		stopSignal:    quit,
		SubscriptionHandler: getLongPollSubscriptionHandler(opts.MaxLongpollTimeoutSeconds,
			clientRequestChan, clientTimeoutChan, opts.Logger.WithField("func", "subscriptionHandler")),
		started: true,
	}
	return &lpManager, nil
}

type clientSubscription struct {
	clientCategoryPair
	// Used to limit events to after a specific time
	LastEventTime time.Time
	// Used in conjunction with LastEventTime to ensue no events are skipped
	LastEventID *uuid.UUID
	// we channel arrays of events since we need to send everything a client
	// cares about in a single channel send.  This makes channel receives a
	// one shot deal.
	Events chan []*Event
}

func newclientSubscriptions(subscriptionCategory string, lastEventTime time.Time, lastEventID *uuid.UUID) (*[]*clientSubscription, chan []*Event, error) {
	u, err := uuid.NewV4()
	if err != nil {
		return nil, nil, err
	}
	events := make(chan []*Event, 1)
	categories := strings.Split(subscriptionCategory, ",")
	subscriptions := make([]*clientSubscription, 0)

	for _, category := range categories {
		subscriptions = append(subscriptions, &clientSubscription{
			clientCategoryPair{ClientUUID: u, SubscriptionCategory: category},
			lastEventTime,
			lastEventID,
			events,
		})
	}
	return &subscriptions, events, nil
}

// PublishData is the json data that LongpollManager.PublishHandler expects.
type PublishData struct {
	Category string      `json:"category"`
	Data     interface{} `json:"data"`
}

// get web handler that has closure around sub chanel and clientTimeout channnel
func getLongPollSubscriptionHandler(maxTimeoutSeconds int, subscriptionRequests chan *clientSubscription,
	clientTimeouts chan<- *clientCategoryPair, logger logrus.FieldLogger) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		timeout, err := strconv.Atoi(r.URL.Query().Get("timeout"))
		logger.Debugf("Handling HTTP request at %s", r.URL)

		flusher, ok := w.(http.Flusher)
		if !ok {
			w.WriteHeader(500)
			w.Write([]byte("Server unsupport flush"))
			return
		}

		// We are going to return json no matter what:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Connection", "Keep-Alive")
		// Don't cache response:
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate") // HTTP 1.1.
		w.Header().Set("Pragma", "no-cache")                                   // HTTP 1.0.
		w.Header().Set("Expires", "0")                                         // Proxies.
		if err != nil || timeout > maxTimeoutSeconds || timeout < 1 {
			logger.Warnf("Invalid or missing 'timeout' param. Must be 1-%d. Got: %q.\n",
				maxTimeoutSeconds, r.URL.Query().Get("timeout"))
			io.WriteString(w, fmt.Sprintf("{\"error\": \"Invalid or missing 'timeout' arg.  Must be 1-%d.\"}", maxTimeoutSeconds))
			return
		}
		category := r.URL.Query().Get("category")
		if len(category) == 0 || len(category) > 1024 {
			logger.Warnf("Invalid or missing subscription 'category', must be 1-1024 characters long.\n")
			io.WriteString(w, "{\"error\": \"Invalid subscription category, must be 1-1024 characters long.\"}")
			return
		}
		// Default to only looking for current events
		lastEventTime := time.Now()
		// since_time is string of milliseconds since epoch
		lastEventTimeParam := r.URL.Query().Get("since_time")
		if len(lastEventTimeParam) > 0 {
			// Client is requesting any event from given timestamp
			// parse time
			var parseError error
			lastEventTime, parseError = millisecondStringToTime(lastEventTimeParam)
			if parseError != nil {
				logger.Warnf("Error parsing since_time arg. Parm Value: %s, Error: %s.\n",
					lastEventTimeParam, parseError)
				io.WriteString(w, "{\"error\": \"Invalid 'since_time' arg.\"}")
				return
			}
		}

		var lastEventID *uuid.UUID

		lastIdParam := r.URL.Query().Get("last_id") // We found the event, we're done.

		if len(lastIdParam) > 0 {
			// further restricting since_time to additionally get events since given last even ID.
			// this handles scenario where multiple events have the same timestamp and we don't
			// want to miss the other events with the same timestamp (issue #19).
			if len(lastEventTimeParam) == 0 {
				logger.Warnf("Invalid request: last_id without since_time.\n")
				io.WriteString(w, "{\"error\": \"Must provide 'since_time' arg when providing 'last_id'.\"}")
				return
			}

			lastEventIDVal, uuidErr := uuid.FromString(lastIdParam)

			if uuidErr != nil {
				logger.Warnf("Invalid request: last_id was not a valid UUID.\n")
				io.WriteString(w, "{\"error\": \"Param 'last_id' was not a valid UUID.\"}")
				return
			}

			lastEventID = &lastEventIDVal
		}

		subscriptions, eventsChannel, err := newclientSubscriptions(category, lastEventTime, lastEventID)
		if err != nil {
			logger.Errorf("Error creating new Subscription: %s.\n", err)
			io.WriteString(w, "{\"error\": \"Error creating new Subscription.\"}")
			return
		}

		var categories []string
		clientUUID := (*subscriptions)[0].clientCategoryPair.ClientUUID.String()

		for _, subscription := range *subscriptions {
			categories = append(categories, subscription.SubscriptionCategory)
			subscriptionRequests <- subscription
		}
		// Listens for connection close and un-register subscription in the
		// event that a client crashes or the connection goes down.  We don't
		// need to wait around to fulfill a subscription if no one is going to
		// receive it
		disconnectNotify := r.Context().Done()
		for {
			select {
			case <-time.After(time.Duration(timeout) * time.Second):
				// Lets the subscription manager know it can discard this request's
				// channel.
				for _, subscription := range *subscriptions {
					clientTimeouts <- &subscription.clientCategoryPair
				}
				timeoutResp := makeTimeoutResponse(time.Now())
				if jsonData, err := json.Marshal(timeoutResp); err == nil {
					io.WriteString(w, string(jsonData))
				} else {
					io.WriteString(w, "{\"error\": \"json marshaller failed\"}")
				}
				return
			case events := <-eventsChannel:
				// Consume event.
				// NOTE: event is actually []Event
				logger.WithFields(
					logrus.Fields{
						"client_uuid": clientUUID,
						"categories":  categories,
					},
				).Infof("Sending events %d events to client", len(events))
				if jsonData, err := json.Marshal(eventResponse{events}); err == nil {
					io.WriteString(w, string(jsonData)+"\n")
					flusher.Flush()
				} else {
					io.WriteString(w, "{\"error\": \"json marshaller failed\"}")
				}
			case <-disconnectNotify:
				// Client connection closed before any events occurred and before
				// the timeout was exceeded.  Tell manager to forget about this
				// client.
				for _, subscription := range *subscriptions {
					clientTimeouts <- &subscription.clientCategoryPair
				}
				return
			}
		}
	}
}

// eventResponse is the json response that carries longpoll events.
type timeoutResponse struct {
	TimeoutMessage string `json:"timeout"`
	Timestamp      int64  `json:"timestamp"`
}

func makeTimeoutResponse(t time.Time) *timeoutResponse {
	return &timeoutResponse{
		TimeoutMessage: "no events before timeout",
		Timestamp:      timeToEpochMilliseconds(t),
	}
}

type clientCategoryPair struct {
	ClientUUID           uuid.UUID
	SubscriptionCategory string
}
