package mirror

import (
	"context"

	"mosn.io/api"
)

var defaultAmplification = 1

func init() {
	api.RegisterStream("mirror", NewMirrorConfig)
}

func NewMirrorConfig(conf map[string]interface{}) (api.StreamFilterChainFactory, error) {
	c := &config{
		Amplification: defaultAmplification,
	}

	if ampValue, ok := conf["amplification"]; ok {
		if amp, ok := ampValue.(float64); ok && amp > 0 {
			c.Amplification = int(amp)
		}
	}
	return c, nil
}

type config struct {
	Amplification int `json:"amplification,omitempty"`
}

func (c *config) CreateFilterChain(ctx context.Context, callbacks api.StreamFilterChainFactoryCallbacks) {
	m := &mirror{amplification: c.Amplification}
	callbacks.AddStreamReceiverFilter(m, api.AfterRoute)
}