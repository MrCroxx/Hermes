package main

import (
	"encoding/json"
	"flag"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"math/rand"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/producer/client"
	"time"
)

type Config struct {
	ZoneID    uint64            `yaml:"ZoneID"`
	Pods      map[uint64]string `yaml:"Pods"`
	Waterline int               `yaml:"Waterline"`
	Frequency uint64            `yaml:"Frequency"`
	Times     int               `yaml:"Times"`
}

func useClient() {
	c := client.NewProducerClient(client.ProducerClientConfig{
		ZoneID: 1,
		Pods: map[uint64]string{
			1: "127.0.0.1:14401",
			2: "127.0.0.1:14402",
			3: "127.0.0.1:14403",
			4: "127.0.0.1:14404",
			5: "127.0.0.1:14405",
		},
	})

	for {
		time.Sleep(time.Millisecond * 10)
		err := c.Send([]string{"窝窝头,一块钱四个,嘿嘿!"})
		if err != nil {
			log.ZAPSugaredLogger().Error(err)
		}
	}
}

func useFlight(cfg Config) {
	commitC := make(chan string)
	errC := client.NewFlight(client.FlightConfig{
		ZoneID:    cfg.ZoneID,
		Pods:      cfg.Pods,
		CommitC:   commitC,
		Waterline: cfg.Waterline,
	})

	go func() {
		for err := range errC {
			log.ZAPSugaredLogger().Error(err)
		}
	}()

	type Data struct {
		F1 float64
		F2 float64
		F3 float64
		F4 float64
		F5 float64
		F6 float64
		F7 float64
	}

	rand.Seed(time.Now().Unix())

	d := 1000 * 1000 / cfg.Frequency

	t := time.NewTicker(time.Duration(d) * time.Microsecond)
	done := make(chan struct{})

	go func() {
		for _ = range t.C {
			d := Data{
				F1: rand.Float64(),
				F2: rand.Float64(),
				F3: rand.Float64(),
				F4: rand.Float64(),
				F5: rand.Float64(),
				F6: rand.Float64(),
				F7: rand.Float64(),
			}
			bs, err := json.Marshal(d)
			if err != nil || bs == nil {
				continue
			}
			for i := 0; i < cfg.Times; i++ {
				commitC <- string(bs)
			}
		}
	}()
	<-done
}

func parseConfigFromFile(path string) (cfg *Config, err error) {
	var f []byte
	if f, err = ioutil.ReadFile(path); err != nil {
		return nil, err
	} else if err = yaml.Unmarshal(f, &cfg); err != nil {
		return nil, err
	}
	return
}

func main() {
	var c = flag.String("c", "", "path to config file")
	flag.Parse()
	cfg, err := parseConfigFromFile(*c)
	if err != nil {
		log.ZAPSugaredLogger().Fatalf("Error raised to parse config, err=%s.", err)
	}
	if cfg.ZoneID == 0 {
		log.ZAPSugaredLogger().Errorf("Please input Zone ID.")
	}
	log.ZAPSugaredLogger().Infof("Hermes Producer Config : %+v.", *cfg)
	useFlight(*cfg)
}
