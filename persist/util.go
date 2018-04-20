package persist

import (
	"bytes"
	"container/list"
	"time"
)

func Timestamp() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

type ReaderCloserWrapper struct {
	*bytes.Reader
}

func NewReaderCloserWrapper(reader *bytes.Reader) *ReaderCloserWrapper {
	return &ReaderCloserWrapper{
		Reader: reader,
	}
}

func (self *ReaderCloserWrapper) Close() error {
	// empty body
	return nil
}

func LogEntryEqual(entry1 *LogEntry, entry2 *LogEntry) bool {
	return (entry1.Term == entry2.Term) &&
		(entry1.Index == entry2.Index) &&
		(entry1.Type == entry2.Type) &&
		(bytes.Compare(entry1.Data, entry2.Data) == 0) &&
		ConfigEqual(entry1.Conf, entry2.Conf)
}

// ListTruncate() removes elements from `e' to the last element in list `l'.
// The range to be removed is [e, l.Back()]. It returns list `l'.
func ListTruncate(l *list.List, e *list.Element) *list.List {
	// remove `e' and all elements after `e'
	var next *list.Element
	for ; e != nil; e = next {
		next = e.Next()
		l.Remove(e)
	}
	return l
}

func ListTruncateHead(l *list.List, e *list.Element) *list.List {
	for elem := l.Front(); elem != e; elem = elem.Next() {
		l.Remove(e)
	}
	return l
}
