package golongpoll

import (
	"container/heap"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/sirupsen/logrus"
)

type subscriptionManager struct {
	clientSubscriptions chan *clientSubscription
	ClientTimeouts      <-chan *clientCategoryPair
	Events              <-chan *Event
	DeletedEvents       <-chan uuid.UUID
	// Contains all client sub channels grouped first by sub id then by
	// client uuid
	ClientSubChannels map[string]map[uuid.UUID]chan<- []*Event
	SubEventBuffer    map[string]*expiringBuffer
	// channel to inform manager to stop running
	Quit         <-chan bool
	shutdownDone chan bool
	// Max allowed timeout seconds when clients requesting a longpoll
	// This is to validate the 'timeout' query param
	MaxLongpollTimeoutSeconds int
	// How big the buffers are (1-n) before events are discareded FIFO
	MaxEventBufferSize int
	// How long events can stay in their eventBuffer
	EventTimeToLiveSeconds int
	// Whether or not to delete an event after the first time it is served via
	// HTTP
	DeleteEventAfterFirstRetrieval bool
	// How often we check for stale event buffers and delete them
	staleCategoryPurgePeriodSeconds int
	// Last time in millisecondss since epoch that performed a stale category purge
	lastStaleCategoryPurgeTime int64
	// PriorityQueue/heap that keeps event buffers in oldest-first order
	// so we can easily delete eventBuffer/categories that have expired data
	bufferPriorityQueue priorityQueue
	// Optional add-on to add behavior like event persistence to longpolling.
	AddOn AddOn

	Logger logrus.FieldLogger
}

// This should be fired off in its own goroutine
func (sm *subscriptionManager) run() error {
	logger := sm.Logger.WithField("sub_component", "run")
	logger.Info("Starting run")
	for {
		// NOTE: we check to see if its time to purge old buffers whenever
		// something happens or a period of inactivity has occurred.
		// An alternative would be to have another goroutine with a
		// select case time.After() but then you'd have concurrency issues
		// with access to the sm.SubEventBuffer and sm.bufferPriorityQueue objs
		// So instead of introducing mutexes we have this uglier manual time check calls
		select {
		case newClient := <-sm.clientSubscriptions:
			sm.handleNewClient(newClient)
			sm.seeIfTimeToPurgeStaleCategories()
		case disconnected := <-sm.ClientTimeouts:
			sm.handleClientDisconnect(disconnected)
			sm.seeIfTimeToPurgeStaleCategories()
		case event := <-sm.Events:
			// Optional hook on publish
			if sm.AddOn != nil {
				sm.AddOn.OnPublish(event)
			}
			sm.handleNewEvent(event)
			sm.seeIfTimeToPurgeStaleCategories()
		case deletedEvent := <-sm.DeletedEvents:
			sm.handleDeletedEvent(deletedEvent)
		case <-time.After(time.Duration(5) * time.Second):
			sm.seeIfTimeToPurgeStaleCategories()
			sm.UpdateMetrics()
		case _ = <-sm.Quit:
			logger.Info("Received quit signal, stopping.")

			// If a Publish() and Shutdown() occur one after the other from the
			// same goroutine, it is random whether or not the quit signal will
			// be seen before the published data, so on shutdown, see if there
			// are additional events before shutting down.
			select {
			case <-time.After(time.Duration(1) * time.Millisecond):
				break
			case event := <-sm.Events:
				if sm.AddOn != nil {
					sm.AddOn.OnPublish(event)
				}
				sm.handleNewEvent(event)
			}

			// optional shutdown callback
			if sm.AddOn != nil {
				sm.AddOn.OnShutdown()
			}

			// signal done shutting down
			close(sm.shutdownDone)

			// break out of our infinite loop/select
			return nil
		}
	}
}

func (sm *subscriptionManager) UpdateMetrics() {
	for category, eventBuffer := range sm.SubEventBuffer {
		EventsInQueue.WithLabelValues(category).Set(float64(eventBuffer.eventBufferPtr.Len()))
	}
}

func (sm *subscriptionManager) seeIfTimeToPurgeStaleCategories() error {
	nowMs := timeToEpochMilliseconds(time.Now())
	if nowMs > (sm.lastStaleCategoryPurgeTime + int64(1000*sm.staleCategoryPurgePeriodSeconds)) {
		sm.lastStaleCategoryPurgeTime = nowMs
		return sm.purgeStaleCategories()
	}
	return nil
}

func (sm *subscriptionManager) handleNewClient(newClient *clientSubscription) error {
	var funcErr error
	logger := sm.Logger.WithField("sub_component", "handleNewClient")

	categoryClients, found := sm.ClientSubChannels[newClient.SubscriptionCategory]
	if !found {
		// first request for this sub category, add client chan map entry
		categoryClients = make(map[uuid.UUID]chan<- []*Event)
		sm.ClientSubChannels[newClient.SubscriptionCategory] = categoryClients
	}
	logger.Debugf("Adding Client (Category: %q Client: %s)\n",
		newClient.SubscriptionCategory, newClient.ClientUUID.String())
	categoryClients[newClient.ClientUUID] = newClient.Events

	// check existing events
	if expiringBuf, found := sm.SubEventBuffer[newClient.SubscriptionCategory]; found {
		// First clean up anything that expired
		sm.checkExpiredEvents(expiringBuf)
		// We have a buffer for this sub category, check for buffered events
		events, err := expiringBuf.eventBufferPtr.GetEventsSince(newClient.LastEventTime,
			sm.DeleteEventAfterFirstRetrieval, newClient.LastEventID)
		logger.Infof("Sending %d events to client %s (last event time: %s)", len(events), newClient.ClientUUID.String(), newClient.LastEventTime)
		if err == nil && len(events) > 0 {
			logger.Debugf("Skip adding client, sending %d events. (Category: %q Client: %s)\n",
				len(events), newClient.SubscriptionCategory, newClient.ClientUUID.String())
			// Send client buffered events.  Client will immediately consume
			// and end long poll request, so no need to have manager store
			newClient.Events <- events
		} else if err != nil {
			funcErr = fmt.Errorf("error getting events from event buffer: %s\n", err)
			logger.Errorf("Error getting events from event buffer: %s.\n", err)
		}
		// Buffer Could have been emptied due to the  DeleteEventAfterFirstRetrieval
		// or EventTimeToLiveSeconds options.
		sm.deleteBufferIfEmpty(expiringBuf, newClient.SubscriptionCategory)
		// NOTE: expiringBuf may now be invalidated (if it was empty/deleted),
		// don't use ref anymore.
	}
	return funcErr
}

func (sm *subscriptionManager) handleClientDisconnect(disconnected *clientCategoryPair) error {
	var funcErr error
	logger := sm.Logger.WithField("sub_component", "handleClientDisconnect")

	if subCategoryClients, found := sm.ClientSubChannels[disconnected.SubscriptionCategory]; found {
		// NOTE:  The delete function doesn't return anything, and will do nothing if the
		// specified key doesn't exist.
		delete(subCategoryClients, disconnected.ClientUUID)
		logger.Infof("Removing Client (Category: %q Client: %s)\n",
			disconnected.SubscriptionCategory, disconnected.ClientUUID.String())
		// Remove the client sub map entry for this category if there are
		// zero clients.  This keeps the ClientSubChannels map lean in
		// the event that there are many categories over time and we
		// would otherwise keep a bunch of empty sub maps
		if len(subCategoryClients) == 0 {
			delete(sm.ClientSubChannels, disconnected.SubscriptionCategory)
		}
	} else {
		// Sub category entry not found.  Weird.  Log this!
		logger.Warnf("Client disconnect for non-existing subscription category: %q\n",
			disconnected.SubscriptionCategory)
		funcErr = fmt.Errorf("Client disconnect for non-existing subscription category: %q\n",
			disconnected.SubscriptionCategory)
	}
	return funcErr
}

func (sm *subscriptionManager) handleDeletedEvent(uid uuid.UUID) error {
	for _, expiringBuf := range sm.SubEventBuffer {
		evt, err := expiringBuf.eventBufferPtr.DequeueEvent(uid)
		if err != nil {
			return err
		}
		if evt != nil {
			return nil
		}
	}
	return nil
}

func (sm *subscriptionManager) handleNewEvent(newEvent *Event) error {
	var funcErr error
	doBufferEvents := true

	logger := sm.Logger.WithField("sub_component", "handleNewEvent")

	// Send event to any listening client's channels
	if clients, found := sm.ClientSubChannels[newEvent.Category]; found && len(clients) > 0 {
		if sm.DeleteEventAfterFirstRetrieval {
			// Configured to delete events from buffer after first retrieval by clients.
			// Now that we already have clients receiving, don't bother
			// buffering the event.
			// NOTE: this is wrapped by condition that clients are found
			// if no clients are found, we queue the event even when we have
			// the delete-on-first option set because no-one has received
			// the event yet.
			doBufferEvents = false
		}
		logger.Debugf("Forwarding event to %d clients. (event: %v)\n", len(clients), newEvent)
		for clientUUID, clientChan := range clients {
			logger.Debugf("Sending event to client: %s\n", clientUUID.String())
			clientChan <- []*Event{newEvent}
		}
	} // else no client subscriptions

	expiringBuf, bufFound := sm.SubEventBuffer[newEvent.Category]
	if doBufferEvents {
		// Add event buffer for this event's subscription category if doesn't exist
		if !bufFound {
			nowMs := timeToEpochMilliseconds(time.Now())
			buf := &eventBuffer{
				make([]*Event, 0),
				sm.MaxEventBufferSize,
				nowMs,
			}
			expiringBuf = &expiringBuffer{
				eventBufferPtr: buf,
				category:       newEvent.Category,
				priority:       nowMs,
			}
			logger.Debugf("Creating new eventBuffer for category: %q",
				newEvent.Category)
			sm.SubEventBuffer[newEvent.Category] = expiringBuf
			sm.priorityQueueUpdateBufferCreated(expiringBuf)
		}
		// queue event in event buffer
		if qErr := expiringBuf.eventBufferPtr.QueueEvent(newEvent); qErr != nil {
			logger.Errorf("Failed to queue event.  err: %s\n", qErr)
			funcErr = fmt.Errorf("Error: failed to queue event.  err: %s\n", qErr)
		} else {
			logger.Tracef("Queued event: %v.\n", newEvent)
			// Queued event successfully
			sm.priorityQueueUpdateNewEvent(expiringBuf, newEvent)
		}
	} else {
		logger.Debugf("DeleteEventAfterFirstRetrieval: skip queue event: %v.\n", newEvent)
	}
	// Perform Event TTL check and empty buffer cleanup:
	if bufFound && expiringBuf != nil {
		sm.checkExpiredEvents(expiringBuf)
		sm.deleteBufferIfEmpty(expiringBuf, newEvent.Category)
		// NOTE: expiringBuf may now be invalidated if it was deleted
	}
	return funcErr
}

func (sm *subscriptionManager) checkExpiredEvents(expiringBuf *expiringBuffer) error {
	if sm.EventTimeToLiveSeconds == forever {
		// Events can never expire. bail out early instead of wasting time.
		return nil
	}
	// determine what time is considered the threshold for expiration
	nowMs := timeToEpochMilliseconds(time.Now())
	expirationTime := nowMs - int64(sm.EventTimeToLiveSeconds*1000)
	return expiringBuf.eventBufferPtr.DeleteEventsOlderThan(expirationTime)
}

func (sm *subscriptionManager) deleteBufferIfEmpty(expiringBuf *expiringBuffer, category string) error {
	if len(expiringBuf.eventBufferPtr.buffer) == 0 {
		sm.Logger.WithField("func", "deleteBufferIfEmpty").Debugf("Deleting empty eventBuffer for category: %q\n", category)
		delete(sm.SubEventBuffer, category)
		sm.priorityQueueUpdateDeletedBuffer(expiringBuf)
	}
	return nil
}

func (sm *subscriptionManager) purgeStaleCategories() error {
	if sm.EventTimeToLiveSeconds == forever {
		// Events never expire, don't bother checking here
		return nil
	}
	logger := sm.Logger.WithField("func", "purgeStaleCategories")
	logger.Debugf("Performing stale category purge.")
	nowMs := timeToEpochMilliseconds(time.Now())
	expirationTime := nowMs - int64(sm.EventTimeToLiveSeconds*1000)
	for sm.bufferPriorityQueue.Len() > 0 {
		topPriority, err := sm.bufferPriorityQueue.peekTopPriority()
		if err != nil {
			// queue is empty (threw empty buffer error) nothing to purge
			break
		}
		if topPriority > expirationTime {
			// The eventBuffer with the oldest most-recent-event-Timestamp is
			// still too recent to be expired, nothing to purge.
			break
		} else { // topPriority <= expirationTime
			// This buffer's most recent event is older than our TTL, so remove
			// the entire buffer.
			if item, ok := heap.Pop(&sm.bufferPriorityQueue).(*expiringBuffer); ok {
				logger.Debugf("Purging expired eventBuffer for category: %q.\n",
					item.category)
				// remove from our category-to-buffer map:
				delete(sm.SubEventBuffer, item.category)
				// invalidate references
				item.eventBufferPtr = nil
			} else {
				logger.Errorf("Found item in bufferPriorityQueue of unexpected type when attempting a TTL purge.\n")
			}
		}
		// will continue until we either run out of heap/queue items or we found
		// a buffer that has events more recent than our TTL window which
		// means we will never find any older buffers.
	}
	return nil
}

// Wraps updates to SubscriptionManager.bufferPriorityQueue when a new
// eventBuffer is created for a given category.  In the event that we don't
// expire events (TTL == FOREVER), we don't bother paying the price of keeping
// the priority queue.
func (sm *subscriptionManager) priorityQueueUpdateBufferCreated(expiringBuf *expiringBuffer) error {
	if sm.EventTimeToLiveSeconds == forever {
		// don't bother keeping track
		return nil
	}
	// NOTE: this call has a complexity of O(log(n)) where n is len of heap
	heap.Push(&sm.bufferPriorityQueue, expiringBuf)
	return nil
}

// Wraps updates to SubscriptionManager.bufferPriorityQueue when a new Event
// is added to an eventBuffer.  In the event that we don't expire events
// (TTL == FOREVER), we don't bother paying the price of keeping the priority
// queue.
func (sm *subscriptionManager) priorityQueueUpdateNewEvent(expiringBuf *expiringBuffer, newEvent *Event) error {
	if sm.EventTimeToLiveSeconds == forever {
		// don't bother keeping track
		return nil
	}
	// Update the priority to be the new event's timestamp.
	// we keep the buffers in order of oldest last-event-timestamp
	// so we can fetch the most stale buffers first when we do
	// purgeStaleCategories()
	//
	// NOTE: this call is O(log(n)) where n is len of heap/priority queue
	sm.bufferPriorityQueue.updatePriority(expiringBuf, newEvent.Timestamp)
	return nil
}

// Wraps updates to SubscriptionManager.bufferPriorityQueue when an eventBuffer
// is deleted after becoming empty.  In the event that we don't
// expire events (TTL == FOREVER), we don't bother paying the price of keeping
// the priority queue.
// NOTE: This is called after an eventBuffer is deleted from sm.SubEventBuffer
// and we want to remove the corresponding buffer item from our priority queue
func (sm *subscriptionManager) priorityQueueUpdateDeletedBuffer(expiringBuf *expiringBuffer) error {
	if sm.EventTimeToLiveSeconds == forever {
		// don't bother keeping track
		return nil
	}
	// NOTE: this call is O(log(n)) where n is len of heap (queue)
	heap.Remove(&sm.bufferPriorityQueue, expiringBuf.index)
	expiringBuf.eventBufferPtr = nil // remove reference to eventBuffer
	return nil
}
