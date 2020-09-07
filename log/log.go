package log

import (
	"fmt"
	"go.uber.org/zap"
)

var (
	logger  *zap.Logger
	sLogger *zap.SugaredLogger
)

func init() {
	// TODO : Complete config
	var err error
	if logger, err = zap.NewDevelopment(); err != nil {
		fs := fmt.Sprintf("error occurred when initializing zap logger, err=%s.", err)
		panic(fs)
	} else {
		sLogger = logger.Sugar()
	}
}

func ZAPLogger() *zap.Logger {
	return logger
}

func ZAPSugaredLogger() *zap.SugaredLogger {
	return sLogger
}

func Sync() error {
	return sLogger.Sync()
}
