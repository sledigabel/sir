package relay

func (r *Relay) Start() {
	go func() {
		r.Listener.Run()
	}()
	go func() {
		r.Backend.Run()
	}()
}

func (r *Relay) Stop() {
	r.Listener.Stop()
	r.Backend.StopAllServers()
}
