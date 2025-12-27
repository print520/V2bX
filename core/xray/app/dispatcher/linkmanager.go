package dispatcher

import (
	sync "sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type ManagedWriter struct {
	writer  buf.Writer
	manager *LinkManager
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	return w.writer.WriteMultiBuffer(mb)
}

func (w *ManagedWriter) Close() error {
	shouldCleanup := w.manager.RemoveWriter(w)
	if shouldCleanup && w.manager.onEmpty != nil {
		// Notify that this LinkManager is empty and can be removed
		// Use a non-blocking approach to avoid goroutine leak
		// The callback should be lightweight and fast
		w.manager.onEmpty()
	}
	return common.Close(w.writer)
}

type LinkManager struct {
	links   map[*ManagedWriter]buf.Reader
	mu      sync.Mutex
	onEmpty func() // Callback to notify when LinkManager becomes empty
}

// SetOnEmpty sets a callback that will be called when the LinkManager becomes empty
func (m *LinkManager) SetOnEmpty(callback func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onEmpty = callback
}

func (m *LinkManager) AddLink(writer *ManagedWriter, reader buf.Reader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.links[writer] = reader
}

// RemoveWriter removes a writer from the LinkManager
// Returns true if the LinkManager is now empty and should be cleaned up
func (m *LinkManager) RemoveWriter(writer *ManagedWriter) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.links, writer)
	return len(m.links) == 0
}

func (m *LinkManager) CloseAll() {
	m.mu.Lock()
	links := make(map[*ManagedWriter]buf.Reader, len(m.links))
	for w, r := range m.links {
		links[w] = r
	}
	m.mu.Unlock()

	for w, r := range links {
		common.Close(w)
		common.Interrupt(r)
	}
}

// IsEmpty returns true if the LinkManager has no active links
func (m *LinkManager) IsEmpty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.links) == 0
}
