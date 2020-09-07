package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

// Hermes configuration struct

type HermesConfig struct {
	PodID                   uint64            `yaml:"PodID",json:"PodID"`                                     // local pod id
	Pods                    map[uint64]string `yaml:"Pods",json:"Pods"`                                       // pod id -> url
	StorageDir              string            `yaml:"StorageDir",json:"StorageDir"`                           // local storage path
	TriggerSnapshotEntriesN uint64            `yaml:"TriggerSnapshotEntriesN",json:"TriggerSnapshotEntriesN"` // entries count when trigger snapshot
	SnapshotCatchUpEntriesN uint64            `yaml:"SnapshotCatchUpEntriesN",json:"SnapshotCatchUpEntriesN"` // entries count for slow followers to catch up before compacting
	MetaZoneOffset          uint64            `yaml:"MetaZoneOffset",json:"MetaZoneOffset"`                   // zone id and node id offset for meta nodes
	MaxPersistN             uint64            `yaml:"MaxPersistN"json:"MaxPersistN"`
	MaxCacheN               uint64            `yaml:"MaxCacheN",json:"MaxCacheN"`
	MaxPushN                uint64            `yaml:"MaxPushN",json:"MaxPushN"`
	PushDataURL             string            `yaml:"PushDataURL",json:"PushDataURL"`
	WebUIPort               uint64            `yaml:"WebUIPort",json:"WebUIPort"`
}

func ParseHermesConfigFromFile(path string) (cfg *HermesConfig, err error) {
	var f []byte
	if f, err = ioutil.ReadFile(path); err != nil {
		return nil, err
	} else if err = yaml.Unmarshal(f, &cfg); err != nil {
		return nil, err
	}
	return
}

// TODO : 重构
type ConfChangeContext struct {
	ZoneID uint64
	PodID  uint64
	NodeID uint64
	URL    string
}
