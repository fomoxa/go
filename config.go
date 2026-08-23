package fomoxa

import "time"

type Config struct {
	HandshakeTimeout    time.Duration
	HeartbeatInterval   time.Duration
	HeartbeatTimeout    time.Duration
	MaxFramesPerTick    int
	ReadBufferSize      int
	MaxPeerBacklog      int
	MaxDatagramsPerTick int
}

func DefaultConfig() Config {
	return Config{
		HandshakeTimeout:    5 * time.Second,
		HeartbeatInterval:   5 * time.Second,
		HeartbeatTimeout:    15 * time.Second,
		MaxFramesPerTick:    64,
		ReadBufferSize:      64 * 1024,
		MaxPeerBacklog:      64,
		MaxDatagramsPerTick: 512,
	}
}

func (c Config) normalized() Config {
	d := DefaultConfig()
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = d.HandshakeTimeout
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = d.HeartbeatInterval
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = d.HeartbeatTimeout
	}
	if c.MaxFramesPerTick <= 0 {
		c.MaxFramesPerTick = d.MaxFramesPerTick
	}
	if c.ReadBufferSize <= 0 {
		c.ReadBufferSize = d.ReadBufferSize
	}
	if c.MaxPeerBacklog <= 0 {
		c.MaxPeerBacklog = d.MaxPeerBacklog
	}
	if c.MaxDatagramsPerTick <= 0 {
		c.MaxDatagramsPerTick = d.MaxDatagramsPerTick
	}
	return c
}
