package web

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/unit"
	"net/http"
)

type WebUI interface {
	Start()
}

type webUI struct {
	pod  unit.Pod
	port uint64
}

func NewWebUI(pod unit.Pod, port uint64) WebUI {
	return &webUI{pod: pod, port: port}
}

func (w *webUI) Start() {
	http.Handle("/", http.FileServer(http.Dir("./ui")))
	http.HandleFunc("/json/metadata", w.serveMetadata)
	http.HandleFunc("/cmd/init", w.serverInit)
	http.HandleFunc("/cmd/transfer-leadership", w.serverTransferLeadership)
	http.HandleFunc("/cmd/add-data-zone", w.serverAddDataZone)
	http.HandleFunc("/cmd/replay-data-zone", w.serverReplayDataZone)
	err := http.ListenAndServe(fmt.Sprintf(":%d", w.port), nil)
	if err != nil {
		log.ZAPSugaredLogger().Fatalf("Error raised when serving http, err=%s.", err)
		panic(err)
	}
}

func (w *webUI) serveMetadata(rsp http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.Error(rsp, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	metadata, err := w.pod.Metadata()
	if err != nil || metadata == nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	bs, err := json.Marshal(*metadata)
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	_, err = fmt.Fprintf(rsp, string(bs))
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func (w *webUI) serverInit(rsp http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(rsp, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	err := w.pod.InitMetaZone()
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	_, err = fmt.Fprintf(rsp, "ok")
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func (w *webUI) serverTransferLeadership(rsp http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(rsp, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	var r struct {
		ZoneID uint64
		NodeID uint64
	}
	if err := json.Unmarshal(body, &r); err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "internal server error", http.StatusInternalServerError)
		return
	}
	err = w.pod.TransferLeadership(r.ZoneID, r.NodeID)
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "internal server error", http.StatusInternalServerError)
		return
	}
	_, err = fmt.Fprintf(rsp, "ok")
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func (w *webUI) serverAddDataZone(rsp http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(rsp, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	var r struct {
		ZoneID uint64
		Nodes  []struct {
			NodeID uint64
			PodID  uint64
		}
	}
	if err := json.Unmarshal(body, &r); err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "internal server error", http.StatusInternalServerError)
		return
	}
	ns := make(map[uint64]uint64)
	for _, n := range r.Nodes {
		ns[n.NodeID] = n.PodID
	}
	err = w.pod.AddRaftZone(r.ZoneID, ns)
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "internal server error", http.StatusInternalServerError)
		return
	}
	_, err = fmt.Fprintf(rsp, "ok")
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func (w *webUI) serverReplayDataZone(rsp http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(rsp, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	var r struct {
		ZoneID uint64
		Index  uint64
	}
	if err := json.Unmarshal(body, &r); err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "internal server error", http.StatusInternalServerError)
		return
	}
	w.pod.ReplayDataZone(r.ZoneID, r.Index)
	_, err = fmt.Fprintf(rsp, "ok")
	if err != nil {
		log.ZAPSugaredLogger().Error(err)
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}
