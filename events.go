package golongpoll

import (
	"errors"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gofrs/uuid"
)

// Event is a longpoll event.  This type has a Timestamp as milliseconds since
// epoch (UTC), a string category, and an arbitrary Data payload.
// The category is the subscription category/topic that clients can listen for
// via longpolling.  The Data payload can be anything that is JSON serializable
// via the encoding/json library's json.Marshal function.
type Event struct {
	// Timestamp is milliseconds since epoch to match javascrits Date.getTime().
	// This is the timestamp when the event was published.
	Timestamp int64 `json:"timestamp"`
	// Category this event belongs to. Clients subscribe to a given category.
	Category string `json:"category"`
	// Event data payload.
	// NOTE: Data can be anything that is able to passed to json.Marshal()
	Data string `json:"data"`
	// Event ID, used in conjunction with Timestamp to get a complete timeline
	// of event data as there could be more than one event with the same timestamp.
	ID uuid.UUID `json:"id"`
	//Expiration, if nil live forever (or according to the TTL of the buffer)
	Expiration *time.Time
}

// eventResponse is the json response that carries longpoll events.
type eventResponse struct {
	Events []*Event `json:"events"`
}

// eventBuffer is a buffer of Events that adds new events to the front/root and
// and old events are removed from the back/tail when the buffer reaches it's
// maximum capacity.
// NOTE: this add-new-to-front/remove-old-from-back behavior is fairly
// efficient since it is implemented as a ring with root.prev being the tail.
// Unlike an array, we don't have to shift every element when something gets
// added to the front, and because our root has a root.prev reference, we can
// quickly jump from the root to the tail instead of having to follow every
// node's node.next field to finally reach the end.
// For more details on our list's implementation, see:
// https://golang.org/src/container/list/list.go
type eventBuffer struct {
	//*list.List
	buffer        []*Event
	MaxBufferSize int
	// keeping track of this allows for more efficient event TTL expiration purges:
	// time in milliseconds since epoch since thats what Event types use
	// for Timestamps
	oldestEventTime int64
}

func newEventWithTime(t time.Time, category string, data string) *Event {
	u, err := uuid.NewV4()

	if err != nil {
		log.Fatalf("Error generating uuid: %q", err)
	}

	return &Event{timeToEpochMilliseconds(t), category, data, u, nil}
}

// QueueEvent adds a new longpoll Event and removes
// the oldest event from the back of the buffer if we're already at maximum
// capacity.
func (eb *eventBuffer) QueueEvent(event *Event) error {
	if event == nil {
		return errors.New("event was nil")
	}

	if len(eb.buffer) > 0 && len(eb.buffer) >= eb.MaxBufferSize {
		log.Tracef("queue: buffer full, removing oldest event: %+v", eb.buffer[0])
		eb.buffer = eb.buffer[1:]
	}

	eb.buffer = append(eb.buffer, event)
	eb.oldestEventTime = eb.buffer[0].Timestamp
	return nil
}

func (eb *eventBuffer) DequeueEvent(uuid uuid.UUID) (*Event, error) {
	if len(eb.buffer) == 0 {
		return nil, nil
	}

	//var prev *list.Element
	//oldestEventTime := eb.oldestEventTime
	log.Tracef("dequeue: list len: %d", len(eb.buffer))

	for i, event := range eb.buffer {
		if event.ID == uuid {
			if i == 0 {
				eb.buffer = eb.buffer[1:]
				eb.oldestEventTime = eb.buffer[0].Timestamp
			} else if i == len(eb.buffer)-1 {
				eb.buffer = eb.buffer[:len(eb.buffer)-1]
			} else {
				eb.buffer = append(eb.buffer[:i], eb.buffer[i+1:]...)
			}
			if len(eb.buffer) == 0 {
				eb.oldestEventTime = 0
			}
			return event, nil
		}
	}
	return nil, nil
}

// GetEventsSnce will return all of the Events in our buffer that occurred after
// the given input time (since).  Returns an error value if there are any
// objects that aren't an Event type in the buffer.  (which would be weird...)
// Optionally removes returned events from the eventBuffer if told to do so by
// deleteFetchedEvents argument.
func (eb *eventBuffer) GetEventsSince(since time.Time,
	deleteFetchedEvents bool, lastEventUUID *uuid.UUID) ([]*Event, error) {
	events := make([]*Event, 0)

	sinceTime := timeToEpochMilliseconds(since)

	for i := len(eb.buffer) - 1; i >= 0; i-- {
		event := eb.buffer[i]
		if event.Expiration != nil && event.Expiration.Before(time.Now()) {
			// expired event, ignore it
			//TODO: remove expired events from buffer
			//eb.buffer = append(eb.buffer[:i], eb.buffer[i+1:]...)
			continue
		}
		if event.Timestamp > sinceTime {
			events = append(events, event)
		} else if event.Timestamp == sinceTime && lastEventUUID != nil && event.ID != *lastEventUUID {
			events = append(events, event)
		} else {
			break
		}
	}

	if deleteFetchedEvents {
		eb.buffer = eb.buffer[:len(eb.buffer)-len(events)]
	}

	// reverse the events so they're in chronological order
	// This is mostly for existing tests compat, but maybe we want to remove this for perf reasons?
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}

	return events, nil
}

func (eb *eventBuffer) DeleteEventsOlderThan(olderThanTimeMs int64) error {
	if len(eb.buffer) == 0 || eb.oldestEventTime > olderThanTimeMs {
		// Either no events or the the oldest event is more recent than
		// olderThanTimeMs, so nothing  could possibly be expired.
		// skip searching list
		return nil
	}

	toDelete := 0

	for i := 0; i < len(eb.buffer); i++ {
		event := eb.buffer[i]
		if event.Timestamp <= olderThanTimeMs {
			toDelete++
		} else {
			break
		}
	}

	if toDelete != 0 {
		lastTs := eb.buffer[toDelete-1].Timestamp
		eb.buffer = eb.buffer[toDelete:]
		if len(eb.buffer) != 0 {
			eb.oldestEventTime = eb.buffer[0].Timestamp
		} else {
			eb.oldestEventTime = lastTs
		}
	}

	return nil
}

func (eb *eventBuffer) DeleteExpiredEvents(olderThanTimeMs int64) error {
	eb.DeleteEventsOlderThan(olderThanTimeMs)

	for i := 0; i < len(eb.buffer); i++ {
		event := eb.buffer[i]
		if event.Expiration != nil && event.Expiration.Before(time.Now()) {
			// expired event, delete it
			eb.buffer = append(eb.buffer[:i], eb.buffer[i+1:]...)
		}
	}

	return nil
}

func (eb *eventBuffer) Len() int {
	return len(eb.buffer)
}
