package pubsub

import (
	"encoding/json"
	"path"
	"sync"
)

type Subscriber interface {
	Send(msg PushMessage)
	ID() string
}

type PushMessage struct {
	Type    string          `json:"type"`
	Pattern string          `json:"pattern,omitempty"`
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type patternEntry struct {
	pattern string
	sub     Subscriber
}

type Hub struct {
	mutex       sync.RWMutex
	channels    map[string]map[Subscriber]struct{}
	patternSubs []patternEntry
}

func NewHub() *Hub {
	return &Hub{
		channels:    make(map[string]map[Subscriber]struct{}),
		patternSubs: make([]patternEntry, 0),
	}
}

func (h *Hub) Subscribe(channel string, sub Subscriber) int {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if _, ok := h.channels[channel]; !ok {
		h.channels[channel] = make(map[Subscriber]struct{})
	}
	h.channels[channel][sub] = struct{}{}

	return h.subCountForSub(sub)
}

func (h *Hub) Unsubscribe(channel string, sub Subscriber) int {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if subs, ok := h.channels[channel]; ok {
		delete(subs, sub)
		if len(subs) == 0 {
			delete(h.channels, channel)
		}
	}

	return h.subCountForSub(sub)
}

func (h *Hub) PSubscribe(pattern string, sub Subscriber) int {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	// Check if already subscribed to exactly this pattern
	already := false
	for _, entry := range h.patternSubs {
		if entry.pattern == pattern && entry.sub.ID() == sub.ID() {
			already = true
			break
		}
	}
	if !already {
		h.patternSubs = append(h.patternSubs, patternEntry{pattern: pattern, sub: sub})
	}
	
	return h.subCountForSub(sub)
}

func (h *Hub) PUnsubscribe(pattern string, sub Subscriber) int {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	newPatterns := make([]patternEntry, 0, len(h.patternSubs))
	for _, entry := range h.patternSubs {
		if entry.pattern == pattern && entry.sub.ID() == sub.ID() {
			continue
		}
		newPatterns = append(newPatterns, entry)
	}
	h.patternSubs = newPatterns

	return h.subCountForSub(sub)
}

func (h *Hub) UnsubscribeAll(sub Subscriber) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for channel, subs := range h.channels {
		delete(subs, sub)
		if len(subs) == 0 {
			delete(h.channels, channel)
		}
	}

	newPatterns := make([]patternEntry, 0, len(h.patternSubs))
	for _, entry := range h.patternSubs {
		if entry.sub.ID() == sub.ID() {
			continue
		}
		newPatterns = append(newPatterns, entry)
	}
	h.patternSubs = newPatterns
}

// subCountForSub must be called with lock held
func (h *Hub) subCountForSub(sub Subscriber) int {
	count := 0
	for _, subs := range h.channels {
		if _, ok := subs[sub]; ok {
			count++
		}
	}
	for _, entry := range h.patternSubs {
		if entry.sub.ID() == sub.ID() {
			count++
		}
	}
	return count
}

func (h *Hub) Publish(channel string, data json.RawMessage) int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	recipients := 0

	// Exact channel subscribers
	if subs, ok := h.channels[channel]; ok {
		for sub := range subs {
			sub.Send(PushMessage{
				Type:    "message",
				Channel: channel,
				Data:    data,
			})
			recipients++
		}
	}

	// Pattern subscribers
	for _, entry := range h.patternSubs {
		matched, err := path.Match(entry.pattern, channel)
		if err == nil && matched {
			entry.sub.Send(PushMessage{
				Type:    "pmessage",
				Pattern: entry.pattern,
				Channel: channel,
				Data:    data,
			})
			recipients++
		}
	}

	return recipients
}

func (h *Hub) Channels() []string {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	channels := make([]string, 0, len(h.channels))
	for channel := range h.channels {
		channels = append(channels, channel)
	}
	return channels
}

func (h *Hub) NumSub(channel string) int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	if subs, ok := h.channels[channel]; ok {
		return len(subs)
	}
	return 0
}
