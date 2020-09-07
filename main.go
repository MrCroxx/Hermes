package main

import (
	"flag"
	"fmt"
	"mrcroxx.io/hermes/component"
	"mrcroxx.io/hermes/config"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"mrcroxx.io/hermes/unit"
	"mrcroxx.io/hermes/web"
	"sort"
	"strconv"
	"strings"
)

const banner = `
 __   __  _______  ______    __   __  _______  _______ 
|  | |  ||       ||    _ |  |  |_|  ||       ||       |
|  |_|  ||    ___||   | ||  |       ||    ___||  _____|
|       ||   |___ |   |_||_ |       ||   |___ | |_____ 
|       ||    ___||    __  ||       ||    ___||_____  |
|   _   ||   |___ |   |  | || ||_|| ||   |___  _____| |
|__| |__||_______||___|  |_||_|   |_||_______||_______|
`

func main() {
	// Sync ZAP log before terminated
	defer log.ZAPSugaredLogger().Sync()

	// Print Hermes Banner
	log.ZAPSugaredLogger().Info(banner)

	// Parse command lind args
	var c = flag.String("c", "", "path to hermes config file")
	flag.Parse()

	if *c == "" {
		fmt.Println("Use -h or --help to get help.")
		return
	}

	// Parse Hermes config file
	cfg, err := config.ParseHermesConfigFromFile(*c)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when parsing hermes config, err=%s.", err)
	}
	log.ZAPSugaredLogger().Debugf("hermes config : %+v", cfg)

	// clean .tmp file
	pkg.CleanTmp(cfg.StorageDir)

	// Initialize Pod
	ec := make(chan error)
	go func() {
		err := <-ec
		log.ZAPSugaredLogger().Fatalf("Pod err : %s.", err)
		panic(err)
	}()
	pod := component.NewPod(*cfg, ec)
	defer pod.Stop()

	if cfg.WebUIPort != 0 {
		webUI := web.NewWebUI(pod, cfg.WebUIPort)
		go webUI.Start()
		log.ZAPSugaredLogger().Infof("start metadata http server at :%d", cfg.WebUIPort)
	}

	startCMD(pod)

}

func startCMD(pod unit.Pod) {
	var cmd string
	for {
		log.ZAPSugaredLogger().Infof("Please input your cmd : ")
		fmt.Scan(&cmd)
		cmds := strings.Split(cmd, ":")
		log.ZAPSugaredLogger().Infof("%+v", cmds)
		op := cmds[0]
		switch op {
		case "all":
			if all, err := pod.All(); err == nil {
				sort.Slice(all, func(i, j int) bool {
					if all[i].ZoneID == all[j].ZoneID {
						return all[i].NodeID < all[j].NodeID
					} else {
						return all[i].ZoneID < all[j].ZoneID
					}
				})
				for _, r := range all {
					log.ZAPSugaredLogger().Infof("%+v", r)
				}

			} else {
				log.ZAPSugaredLogger().Errorf("%s", err)
			}
		case "add":
			zoneID, _ := strconv.Atoi(cmds[1])
			nodes := make(map[uint64]uint64)
			for i := 2; i < len(cmds); i += 2 {
				nid, _ := strconv.Atoi(cmds[i])
				pid, _ := strconv.Atoi(cmds[i+1])
				nodes[uint64(nid)] = uint64(pid)
			}
			if err := pod.AddRaftZone(uint64(zoneID), nodes); err == nil {
				log.ZAPSugaredLogger().Infof("ok")
			} else {
				log.ZAPSugaredLogger().Errorf("%s", err)
			}
		case "tl":
			zoneID, _ := strconv.Atoi(cmds[1])
			nodeID, _ := strconv.Atoi(cmds[2])
			if err := pod.TransferLeadership(uint64(zoneID), uint64(nodeID)); err == nil {
				log.ZAPSugaredLogger().Infof("ok")
			} else {
				log.ZAPSugaredLogger().Errorf("%s", err)
			}
		case "wk":
			nodeID, _ := strconv.Atoi(cmds[1])
			pod.WakeUpNode(uint64(nodeID))
			log.ZAPSugaredLogger().Infof("ok")
		default:
			log.ZAPSugaredLogger().Infof("none")
		}
	}
}

/*

go run mrcroxx.io/hermes -c f:\hermes-1.yaml

go run mrcroxx.io/hermes -c f:\hermes-2.yaml

go run mrcroxx.io/hermes -c f:\hermes-3.yaml

go run mrcroxx.io/hermes -c f:\hermes-4.yaml

go run mrcroxx.io/hermes -c f:\hermes-5.yaml

go run mrcroxx.io/hermes/producer

go run mrcroxx.io/hermes/consumer

go build mrcroxx.io/hermes && go build mrcroxx.io/hermes/producer && go build mrcroxx.io/hermes/consumer

hermes.exe -c f:\hermes-1.yaml

hermes.exe -c f:\hermes-2.yaml

hermes.exe -c f:\hermes-3.yaml

hermes.exe -c f:\hermes-4.yaml

hermes.exe -c f:\hermes-5.yaml

producer.exe -c f:\hermes-producer-1.yaml

producer.exe -c f:\hermes-producer-2.yaml

consumer.exe

*/
