package logging

import "fmt"

func (a *Adapter) Debugf(format string, args ...any) {
	if a.slogLogger != nil {
		a.slogLogger.Debug(fmt.Sprintf(format, args...))

		return
	}

	if a.hostLogger != nil {
		a.hostLogger.Debugf(format, args...)
	}
}

func (a *Adapter) Infof(format string, args ...any) {
	if a.slogLogger != nil {
		a.slogLogger.Info(fmt.Sprintf(format, args...))

		return
	}

	if a.hostLogger != nil {
		a.hostLogger.Infof(format, args...)
	}
}

func (a *Adapter) Warnf(format string, args ...any) {
	if a.slogLogger != nil {
		a.slogLogger.Warn(fmt.Sprintf(format, args...))

		return
	}

	if a.hostLogger != nil {
		a.hostLogger.Warnf(format, args...)
	}
}

func (a *Adapter) Errorf(format string, args ...any) {
	if a.slogLogger != nil {
		a.slogLogger.Error(fmt.Sprintf(format, args...))

		return
	}

	if a.hostLogger != nil {
		a.hostLogger.Errorf(format, args...)
	}
}
