package log

import (
	"go.uber.org/zap"
)

type Logger struct {
	*zap.SugaredLogger
}

func New(logger *zap.SugaredLogger) Logger {
	return Logger{
		SugaredLogger: logger,
	}
}
