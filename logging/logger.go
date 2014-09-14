package logging

import (
    "github.com/hhkbp2/go-logger/logger"
)

func init() {
    logger.SetConsole(false)
    logger.SetLevel(logger.DEBUG)
    logger.SetRollingFile("./testlog", "test.log", 10, 500, logger.MB)
}

type LogLevel uint8

const (
    CRITICAL LogLevel = 50
    FATAL             = CRITICAL
    ERROR             = 40
    WARNING           = 30
    WARN              = WARNING
    INFO              = 20
    DEBUG             = 10
    NOTSET            = 0
)

type Logger interface {
    Critical(format string, args ...interface{})
    Error(format string, args ...interface{})
    Warning(format string, args ...interface{})
    Info(format string, args ...interface{})
    Debug(format string, args ...interface{})
}

type FileLogger struct {
    name string
}

func (self *FileLogger) Critical(format string, args ...interface{}) {
    logger.Fatalf(self.name+" "+format, args...)
}

func (self *FileLogger) Error(format string, args ...interface{}) {
    logger.Errorf(self.name+" "+format, args...)
}

func (self *FileLogger) Warning(format string, args ...interface{}) {
    logger.Warnf(self.name+" "+format, args...)
}

func (self *FileLogger) Info(format string, args ...interface{}) {
    logger.Infof(self.name+" "+format, args...)
}

func (self *FileLogger) Debug(format string, args ...interface{}) {
    logger.Debugf(self.name+" "+format, args...)
}

func GetLogger(name string) Logger {
    return &FileLogger{
        name: name,
    }
}
