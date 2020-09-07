window.onload = function () {
    var app = new Vue({
        el: '#app',
        data() {
            let validateZoneIDNotExists = (rule, value, callback) => {
                zids = new Set(this.zoneIDs());
                if (zids.has(value)) return callback(new Error('ZoneID already exists'));
                return callback();
            };
            let validateNode = (rule, value, callback) => {
                let nids = new Set(this.nodeIDs(null));
                if (nids.has(value.NodeID)) return callback("NodeID already exists");
                return callback();
            };
            let validateRC = (rule, value, callback) => {
                if (value < 3) return callback("replication count must be greater than 3");
                return callback();
            };
            return {
                message: 'Hello Hermes!',
                metadata: null,
                ticker: null,
                tab: "overview",
                config: [],
                datazones: {},
                metazone: [],
                metrics: {},
                metrics_collapse: [],
                tl: null,
                adz: null,
                rdz: null,
                tlRules: {
                    ZoneID: [
                        {required: true, trigger: "change"}
                    ],
                    NodeID: [
                        {required: true, trigger: "change"}
                    ]
                },
                adzRules: {
                    ZoneID: [
                        {required: true, trigger: "change"},
                        {type: "number", trigger: "change"},
                        {validator: validateZoneIDNotExists, trigger: "change"}
                    ],
                    RC: [
                        {validator: validateRC, trigger: "change"}
                    ],
                },
                rdzRules: {
                    ZoneID: [
                        {required: true, trigger: "change"}
                    ],
                    Index: [
                        {required: true, trigger: "change"},
                        {type: "number", trigger: "change"}
                    ]
                },
                validateNode: validateNode,
            }
        },
        created: function () {
            console.log("Hello Hermes!");
            this.ticker = setInterval(this.getMetadata, 1000);
        },
        methods: {
            handleTab(key, keyPath) {
                this.tab = key;
                switch (key) {
                    case "transfer-leadership":
                        this.tl = {
                            ZoneID: null,
                            NodeID: null,
                        };
                        this.$nextTick(() => this.clearValidate('tl'));
                        break;
                    case "add-data-zone":
                        this.adz = {
                            ZoneID: null,
                            RC: 0,
                            Nodes: [],
                        };
                        this.$nextTick(() => this.clearValidate('adz'));
                        break;
                    case "replay-data-zone":
                        this.rdz = {
                            ZoneID: null,
                            Index: null,
                        };
                        this.$nextTick(() => this.clearValidate('rdz'));
                        break;
                }
            },
            clearValidate(ref) {
                this.$refs[ref].clearValidate();
            },
            date2string(date) {
                return `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')} ${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}`
            },
            isDead({row, rowIndex}) {
                if (row.Heartbeat.getTime() < ((new Date()).getTime() - 10 * 1000)) {
                    return 'danger-row'
                }
                return ''
            },
            alertError(err) {
                this.$message({
                    message: err,
                    type: 'error',
                });
            },
            initHermes() {
                axios
                    .post("/cmd/init")
                    .then(response => {
                        this.$message({
                            message: 'success',
                            type: 'success',
                        });
                    })
                    .catch(this.alertError)
            },
            submitTL() {
                axios
                    .post("/cmd/transfer-leadership", this.tl)
                    .then(response => {
                        this.tab = 'overview';
                        this.$message({
                            message: 'success',
                            type: 'success',
                        });
                    })
                    .catch(this.alertError)
            },
            adzMoreNode() {
                this.adz.Nodes.push({
                    NodeID: null,
                    PodID: null,
                    key: Date.now(),
                });
                this.adz.RC = this.adz.Nodes.length;
            },
            adzDeleteNode(index) {
                this.adz.Nodes.splice(index, 1);
                this.adz.RC = this.adz.Nodes.length;
            },
            submitADZ() {
                axios
                    .post("/cmd/add-data-zone", this.adz)
                    .then(response => {
                        this.tab = 'overview';
                        this.$message({
                            message: 'success',
                            type: 'success',
                        });
                    })
                    .catch(this.alertError)
            },
            submitRDZ() {
                axios
                    .post("/cmd/replay-data-zone", this.rdz)
                    .then(response => {
                        this.tab = 'overview';
                        this.$message({
                            message: 'success',
                            type: 'success',
                        });
                    })
                    .catch(this.alertError)
            },
            getMetadata() {
                axios
                    .get("/json/metadata")
                    .then(response => {
                        this.metadata = response.data;
                        // parse config
                        config = this.metadata.Config;
                        tmp = [];
                        for (var k in config) {
                            if (k != "Pods") {
                                tmp.push({"key": k, "val": config[k]})
                            } else {
                                s = "";
                                for (var i in config[k]) {
                                    s += "<" + i + " : " + config[k][i] + "> ";
                                }
                                tmp.push({"key": k, "val": s})

                            }
                        }
                        this.config = tmp;
                        // parse zone
                        metazone = [];
                        datazones = {};
                        metaZoneOffset = config.MetaZoneOffset;
                        for (var i in this.metadata.RaftRecords) {
                            // raw raft record
                            rrr = this.metadata.RaftRecords[i];

                            rr = {
                                ZoneID: rrr.ZoneID,
                                NodeID: rrr.NodeID,
                                PodID: rrr.PodID,
                                IsLeader: (rrr.IsLeader) ? ("√") : (""),
                                Heartbeat: new Date(rrr.Heartbeat),
                                HeartbeatStr: this.date2string(new Date(rrr.Heartbeat)),
                            };
                            if (rrr.Extra !== "") {
                                extra = JSON.parse(rrr.Extra);
                                rr.Deleted = `${extra.DeletedIndex} [${extra.DeletedIndex}]`;
                                rr.Persisted = `${extra.PersistedIndex - extra.DeletedIndex} [${extra.PersistedIndex}]`;
                                rr.Cached = `${extra.CachedIndex - extra.PersistedIndex} [${extra.CachedIndex}]`;
                                rr.Fresh = `${extra.FreshIndex - extra.CachedIndex} [${extra.FreshIndex}]`;
                            }

                            if (rr.ZoneID === metaZoneOffset) {
                                metazone.push(rr);
                                continue;
                            }
                            zid = rr.ZoneID;
                            if (datazones[zid] === undefined) {
                                datazones[zid] = [];
                            }
                            datazones[zid].push(rr);

                            // metrics
                            if (rrr.IsLeader && rr.ZoneID !== metaZoneOffset) {
                                if (this.metrics[rr.ZoneID] == null) {
                                    // init
                                    let d = new Date();
                                    this.metrics[rr.ZoneID] = {
                                        "index": {
                                            "Deleted": [],
                                            "Persisted": [],
                                            "Cached": [],
                                            "Fresh": [],
                                        },
                                        "size": {
                                            "Persisted": [],
                                            "Cached": [],
                                            "Fresh": [],
                                        },
                                        "acc": {
                                            "Deleted": [],
                                            "Persisted": [],
                                            "Cached": [],
                                            "Fresh": [],
                                        }
                                    };
                                    this.metrics_collapse[rr.ZoneID] = [];
                                    for (let k = 0; k < 60; k++) {
                                        this.metrics[rr.ZoneID]["index"]["Deleted"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["index"]["Persisted"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["index"]["Cached"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["index"]["Fresh"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["size"]["Persisted"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["size"]["Cached"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["size"]["Fresh"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["acc"]["Deleted"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["acc"]["Persisted"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["acc"]["Cached"].unshift({'t': d, 'v': 0});
                                        this.metrics[rr.ZoneID]["acc"]["Fresh"].unshift({'t': d, 'v': 0});
                                        d = new Date(d.getTime() - 1000);
                                    }
                                }

                                if (rrr.Extra !== "") {
                                    extra = JSON.parse(rrr.Extra);
                                    let d = new Date();

                                    this.metrics[rr.ZoneID]["index"]["Deleted"].shift();
                                    this.metrics[rr.ZoneID]["index"]["Persisted"].shift();
                                    this.metrics[rr.ZoneID]["index"]["Cached"].shift();
                                    this.metrics[rr.ZoneID]["index"]["Fresh"].shift();
                                    this.metrics[rr.ZoneID]["size"]["Persisted"].shift();
                                    this.metrics[rr.ZoneID]["size"]["Cached"].shift();
                                    this.metrics[rr.ZoneID]["size"]["Fresh"].shift();


                                    this.metrics[rr.ZoneID]["index"]["Deleted"].push({'t': d, 'v': extra.DeletedIndex});
                                    this.metrics[rr.ZoneID]["index"]["Persisted"].push({'t': d, 'v': extra.PersistedIndex});
                                    this.metrics[rr.ZoneID]["index"]["Cached"].push({'t': d, 'v': extra.CachedIndex});
                                    this.metrics[rr.ZoneID]["index"]["Fresh"].push({'t': d, 'v': extra.FreshIndex});
                                    this.metrics[rr.ZoneID]["size"]["Persisted"].push({'t': d, 'v': extra.PersistedIndex - extra.DeletedIndex});
                                    this.metrics[rr.ZoneID]["size"]["Cached"].push({'t': d, 'v': extra.CachedIndex - extra.PersistedIndex});
                                    this.metrics[rr.ZoneID]["size"]["Fresh"].push({'t': d, 'v': extra.FreshIndex - extra.CachedIndex});

                                    this.metrics[rr.ZoneID]["acc"]["Deleted"].shift();
                                    this.metrics[rr.ZoneID]["acc"]["Persisted"].shift();
                                    this.metrics[rr.ZoneID]["acc"]["Cached"].shift();
                                    this.metrics[rr.ZoneID]["acc"]["Fresh"].shift();

                                    if (this.metrics[rr.ZoneID]['acc-on']) {
                                        let s = null;
                                        s = this.metrics[rr.ZoneID]["index"]["Deleted"].slice(-2);
                                        this.metrics[rr.ZoneID]["acc"]["Deleted"].push({'t': d, 'v': s[1]['v'] - s[0]['v']});
                                        s = this.metrics[rr.ZoneID]["index"]["Persisted"].slice(-2);
                                        this.metrics[rr.ZoneID]["acc"]["Persisted"].push({'t': d, 'v': s[1]['v'] - s[0]['v']});
                                        s = this.metrics[rr.ZoneID]["index"]["Cached"].slice(-2);
                                        this.metrics[rr.ZoneID]["acc"]["Cached"].push({'t': d, 'v': s[1]['v'] - s[0]['v']});
                                        s = this.metrics[rr.ZoneID]["index"]["Fresh"].slice(-2);
                                        this.metrics[rr.ZoneID]["acc"]["Fresh"].push({'t': d, 'v': s[1]['v'] - s[0]['v']});
                                    }
                                    this.metrics[rr.ZoneID]['acc-on'] = true;
                                    let data_index = [
                                        this.metrics[rr.ZoneID]["index"]["Deleted"],
                                        this.metrics[rr.ZoneID]["index"]["Persisted"],
                                        this.metrics[rr.ZoneID]["index"]["Cached"],
                                        this.metrics[rr.ZoneID]["index"]["Fresh"],
                                    ];
                                    let data_size = [
                                        this.metrics[rr.ZoneID]["size"]["Persisted"],
                                        this.metrics[rr.ZoneID]["size"]["Cached"],
                                        this.metrics[rr.ZoneID]["size"]["Fresh"],
                                    ];
                                    let data_acc = [
                                        this.metrics[rr.ZoneID]["acc"]["Deleted"],
                                        this.metrics[rr.ZoneID]["acc"]["Persisted"],
                                        this.metrics[rr.ZoneID]["acc"]["Cached"],
                                        this.metrics[rr.ZoneID]["acc"]["Fresh"],
                                    ];


                                    if (document.getElementById('metric-index-' + `${rr.ZoneID}`))
                                        MG.data_graphic({
                                            title: "Metrics(Index) - Data Zone " + `${rr.ZoneID}`,
                                            data: data_index,
                                            width: 400,
                                            height: 200,
                                            target: '#metric-index-' + `${rr.ZoneID}`,
                                            legend: ['Deleted', 'Persisted', 'Cached', 'Fresh'],
                                            legend_target: '#legend-index-' + `${rr.ZoneID}`,
                                            x_accessor: 't',
                                            y_accessor: 'v',
                                        });

                                    if (document.getElementById('metric-size-' + `${rr.ZoneID}`))
                                        MG.data_graphic({
                                            title: "Metrics(Size) - Data Zone " + `${rr.ZoneID}`,
                                            data: data_size,
                                            width: 400,
                                            height: 200,
                                            target: '#metric-size-' + `${rr.ZoneID}`,
                                            legend: ['Persisted', 'Cached', 'Fresh'],
                                            legend_target: '#legend-size-' + `${rr.ZoneID}`,
                                            x_accessor: 't',
                                            y_accessor: 'v',
                                        });

                                    if (document.getElementById('metric-acc-' + `${rr.ZoneID}`))
                                        MG.data_graphic({
                                            title: "Metrics(Increment) - Data Zone " + `${rr.ZoneID}`,
                                            data: data_acc,
                                            width: 400,
                                            height: 200,
                                            target: '#metric-acc-' + `${rr.ZoneID}`,
                                            legend: ['Deleted', 'Persisted', 'Cached', 'Fresh'],
                                            legend_target: '#legend-acc-' + `${rr.ZoneID}`,
                                            x_accessor: 't',
                                            y_accessor: 'v',
                                        });
                                }
                            }

                        }
                        this.datazones = datazones;
                        this.metazone = metazone;
                    })
                    .catch(err => {
                        console.log(err);
                    });
                // this.$http.get("/json/metadata").then();
            },
            podIDs() {
                let pids = new Set();
                this.metadata.RaftRecords.forEach(rr => {
                    pids.add(rr["PodID"]);
                });
                return Array.from(pids).sort();
            },
            zoneIDs() {
                let zids = new Set();
                this.metadata.RaftRecords.forEach(rr => {
                    zids.add(rr["ZoneID"]);
                });
                return Array.from(zids).sort();
            },
            dataZoneIDs() {
                let zids = new Set();
                this.metadata.RaftRecords.forEach(rr => {
                    if (rr["ZoneID"] !== this.metadata.Config.MetaZoneOffset)
                        zids.add(rr["ZoneID"]);
                });
                return Array.from(zids).sort();
            },
            nodeIDs(zid) {
                if (zid == null) {
                    let nids = new Set();
                    this.metadata.RaftRecords.forEach(rr => {
                        nids.add(rr["NodeID"]);
                    });
                    return Array.from(nids).sort();
                }
                let nids = new Set();
                this.metadata.RaftRecords.forEach(rr => {
                    if (rr["ZoneID"] == zid) {
                        nids.add(rr["NodeID"]);
                    }
                });
                return Array.from(nids).sort();
            },
            metricsV(zid) {
                return this.metrics_collapse[zid].indexOf(zid.toString()) >= 0;
            }
        },
        computed: {
            loading() {
                return this.metadata === null;
            }
        }
    });
};