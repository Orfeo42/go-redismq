package redismq

type RedisMqConfig struct {
	Group    string
	Addr     string
	Password string
	Database int
}

func (cfg RedisMqConfig) validate() error {
	if len(cfg.Addr) == 0 {
		return ErrConfigAddrBlank
	}

	if len(cfg.Group) == 0 {
		return ErrConfigGroupBlank
	}

	return nil
}
