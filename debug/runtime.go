package debug

import (
    "github.com/hhkbp2/rafted/logging"
    "runtime"
)

type StackInfo struct {
    File string
    Line int
}

type StackWriter interface {
    Write([]*StackInfo)
}

var (
    MaxCallStackNumber int = 1000
)

func LogCallStack(logger logging.Logger) {
    PrintCallStack(NewLogStackWriter(logger))
}

func PrintCallStack(writer StackWriter) {
    writer.Write(GetCallStack())
}

func GetCallStack() []*StackInfo {
    // sufficient for most cases
    stackInfos := make([]*StackInfo, 0)
    for i := 0; i < MaxCallStackNumber; i++ {
        _, file, line, ok := runtime.Caller(i)
        if !ok {
            return stackInfos
        }
        info := &StackInfo{
            File: file,
            Line: line,
        }
        stackInfos = append(stackInfos, info)
    }
    return stackInfos
}

type LogStackWriter struct {
    logger logging.Logger
}

func NewLogStackWriter(logger logging.Logger) *LogStackWriter {
    return &LogStackWriter{
        logger: logger,
    }
}

func (self *LogStackWriter) Write(stackInfos []*StackInfo) {
    for _, info := range stackInfos {
        self.logger.Debug("%s:%d", info.File, info.Line)
    }
}
