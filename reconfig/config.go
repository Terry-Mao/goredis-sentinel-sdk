package main

import (
	log "code.google.com/p/log4go"
	"flag"
	"github.com/Terry-Mao/goconf"
)

var (
	conf     *Config
	gconf    *goconf.Config
	confFile string
)

func init() {
	flag.StringVar(&confFile, "c", "./message.conf", " set message config file path")
}

// Config struct
type Config struct {
	Log      string   `goconf:"base:log"`
	Master   string   `goconf:"base:master"`
	Sentinel []string `goconf:"base:sentinel:,"`
	ZK       []string `goconf:"base:zk:,"`
	ZKPath   string   `goconf:"base:zk.path"`
	Node     string   `goconf:"base:node"`
	Weight   int      `goconf:"base:weight"`
}

// initConfig parse config file into Config.
func initConfig() error {
	gconf = goconf.New()
	if err := gconf.Parse(confFile); err != nil {
		return err
	}
	conf = &Config{
		// base
		Log:      "./log/xml",
		Master:   "mymaster",
		Sentinel: []string{},
		ZK:       []string{},
		ZKPath:   "/redis-cluster/service_name",
		Node:     "node1",
		Weight:   100,
	}
	if err := gconf.Unmarshal(conf); err != nil {
		return err
	}
	return nil
}

// saveConfig save config file.
func saveConfig() {
	if err := gconf.Save(confFile); err != nil {
		log.Error("conf.Save(\"%s\") error(%v)", confFile, err)
	}
	log.Info("save config file: \"%s\"", confFile)
}
