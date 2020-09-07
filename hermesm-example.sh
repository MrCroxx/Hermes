#!/bin/bash

# custom node config
cluster=('ivic@192.168.106.241' 'ivic@192.168.106.242' 'ivic@192.168.106.243' 'ivic@192.168.106.244')
producer=('ivic@192.168.106.240' 'ivic@192.168.106.240' 'ivic@192.168.106.240' 'ivic@192.168.106.240')
consumer='ivic@192.168.106.240'

# custom hermes config
# hermes worker cluster
TCPPort=14400
StorageDirBase="${HOME}"
TriggerSnapshotEntriesN=10000
SnapshotCatchUpEntriesN=10000
MetaZoneOffset=10000
MaxPersistN=10000
MaxCacheN=1000
MaxPushN=100000
WebUIPort=8080
# hermes producer
Waterline=10000
Frequency=100
Times=10
# hermes consumer
ConsumerPort=15500

# script
case $1 in
        'start')
                echo "start hermes-consumer on ${consumer}"
                ssh $consumer screen -S hermes-consumer -dm "${HOME}/hermes-consumer"
                i=0
                for host in ${cluster[*]}
                do
                        let i++
                        echo "start hermes on ${host}"
                        ssh $host screen -S hermes -dm "${HOME}/hermes -c ${HOME}/hermes-${i}.yaml"
                done
                ;;
        'stop')
                for host in ${cluster[*]}
                do
                        echo "stop hermes on ${host}"
                        ssh $host screen -S hermes -X quit
                done
                echo "stop hermes-consumer on $consumer"
                ssh $consumer screen -S hermes-consumer -X quit
                ;;
        'ls')
                for host in ${cluster[*]}
                do
                        if [[ $(sed '1d' <<< $(ssh $host 'screen -ls')) =~ 'hermes' ]]
                        then
                                echo "hermes worker running on ${host}"
                        fi
                done

                i=0
                for host in ${producer[*]}
                do
                        let i++
                        pname="hermes-producer-${i}"
                        if [[ $(sed '1d' <<< $(ssh $host 'screen -ls')) =~ $pname ]]
                        then
                                echo "hermes producer running on ${host}"
                        fi
                done
                if [[ $(sed '1d' <<< $(ssh $consumer 'screen -ls')) =~ 'hermes-consumer' ]]
                then
                        echo "hermes consumer running on ${consumer}"
                fi
                ;;
        'config')
                cd ~
                i=0
                for host in ${cluster[*]}
                do
                        let i++
                        cfg="hermes-${i}.yaml"
                        echo "generate ${cfg}"

                        echo "PodID : ${i}" > $cfg
                        echo "Pods:" >> $cfg
                        k=0
                        for h in ${cluster[*]}
                        do
                          let k++
                          echo "  ${k} : ${h#*@}:${TCPPort}" >> $cfg
                        done
                        echo "" >> $cfg
                        echo "StorageDir : ${StorageDirBase}/hermes-${i}/" >> $cfg
                        echo "TriggerSnapshotEntriesN : ${TriggerSnapshotEntriesN}" >> $cfg
                        echo "SnapshotCatchUpEntriesN : ${SnapshotCatchUpEntriesN}" >> $cfg
                        echo "MetaZoneOffset : ${MetaZoneOffset}" >> $cfg
                        echo "MaxPersistN : ${MaxPersistN}" >> $cfg
                        echo "MaxCacheN : ${MaxCacheN}" >> $cfg
                        echo "MaxPushN : ${MaxPushN}" >> $cfg
                        echo "PushDataURL : http://${consumer#*@}:${ConsumerPort}" >> $cfg
                        echo "WebUIPort : ${WebUIPort}" >> $cfg
                done

                i=0
                for host in ${producer[*]}
                do
                        let i++
                        cfg="hermes-producer-${i}.yaml"
                        echo "generate ${cfg}"

                        echo "ZoneID : ${i}" > $cfg
                        echo "Pods:" >> $cfg
                        k=0
                        for h in ${cluster[*]}
                        do
                          let k++
                          echo "  ${k} : ${h#*@}:${TCPPort}" >> $cfg
                        done
                        echo "" >> $cfg
                        echo "Waterline : ${Waterline}" >> $cfg
                        echo "Frequency : ${Frequency}" >> $cfg
                        echo "Times : ${Times}" >> $cfg
                done
                ;;
        'deploy')
                echo "build & install hermes"
                cd $GOPATH/src/mrcroxx.io/hermes
                go install mrcroxx.io/hermes
                go install mrcroxx.io/hermes/producer
                go install mrcroxx.io/hermes/consumer

                i=0
                for host in ${cluster[*]}
                do
                        let i++
                        echo "deploy hermes on ${host}"
                        scp -q $GOPATH/bin/hermes ${host}:${HOME}/
                        scp -q ${HOME}/hermes-${i}.yaml ${host}:${HOME}/
                        scp -q -r $GOPATH/src/mrcroxx.io/hermes/ui ${host}:${HOME}/
                done

                i=0
                for host in ${producer[*]}
                do
                        let i++
                        echo "deploy hermes-producer on ${host}"
                        scp -q $GOPATH/bin/producer ${host}:${HOME}/hermes-producer
                        scp -q ${HOME}/hermes-producer-${i}.yaml ${host}:${HOME}/
                done

                echo "deploy hermes-consumer on ${consumer}"
                scp -q $GOPATH/bin/consumer ${consumer}:${HOME}/hermes-consumer
                ;;
        'clean')
                i=0
                for host in ${cluster[*]}
                do
                        let i++
                        echo "clean hermes on ${host}"
                        ssh $host rm -rf ${HOME}/hermes-${i} ${HOME}/hermes ${HOME}/hermes-${i}.yaml ${HOME}/ui
                done

                i=0
                for host in ${producer[*]}
                do
                        let i++
                        echo "clean hermes-producer on ${host}"
                        ssh $host rm -f ${HOME}/hermes-producer ${HOME}/hermes-producer-${i}.yaml
                done

                echo "clean hermes-consumer on ${consumer}"
                ssh $consumer rm -f ${HOME}/hermes-consumer
                ;;
        *)
                echo "start | stop | ls | config | deploy | clean"
                ;;
esac