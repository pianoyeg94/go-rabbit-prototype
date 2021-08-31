package rabbit

func newErrorSignaling() errorSignaling {
	return errorSignaling{
		errorSink:   make(chan error),
		fatalErrors: make(chan error, 1),
	}
}

type errorSignaling struct {
	errorSink   chan error
	fatalErrors chan error
}

func (e errorSignaling) signalError(err error) {
	select {
	case e.errorSink <- err:
	default:
	}
}

func (e errorSignaling) signalFatalError(err error) {
	select {
	case e.fatalErrors <- err:
	default:
	}
}
