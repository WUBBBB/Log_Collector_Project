package main

import (
	"fmt"

	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"main.go/common"
	"main.go/etcd"
	"main.go/kafka"
	"main.go/tailfile"
)

// 日志收集的客户端
// 类似开源项目：filebeat
// 收集指定目录下的日志文件，发送到kafka中

// 现在的技能包：
// 往kafka发数据
// 使用tail读日志文件

// 整个logagent的配置结构体
type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

// `[{"path":"F:/LogCollectorProject/err_logs/video_err.log","topic":"video_log"}
// ,{"path":"F:/LogCollectorProject/err_logs/sign_in_err.log","topic":"sign_in_log"}]`

type KafkaConfig struct {
	Address string `ini:"address"`
	// Topci    string `ini:"topic"`
	ChanSize int64 `ini:"chan_size"`
}

type CollectConfig struct {
	// LogfilePath string `ini:"logfile_path"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

func run() {
	select {}
}

func main() {
	// 获取本地IP，为后续去etcd配置文件做好准备
	ip, err := common.GetOutboundIP()
	if err != nil {
		logrus.Errorf("get ip is failed, err:%v", err)
		return
	}
	var configObj = new(Config)
	// 0.	读配置文件 `go-ini`		获得连接端口、初始化设置等信息
	err = ini.MapTo(configObj, "./config/config.ini")
	if err != nil {
		logrus.Error("load config failed, err: ", err)
		return
	}
	fmt.Printf("%v\n", configObj)
	// 1.	初始化连接kafka
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Error("init kafka failed, err: ", err)
		return
	}
	logrus.Info("init kafka success!")

	// 初始化etcd连接
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed, err:%v", err)
		return
	}
	// 从etcd中拉取要搜集日志的配置
	collectKey := fmt.Sprintf(configObj.EtcdConfig.CollectKey, ip)
	allConf, err := etcd.GetConf(collectKey)
	if err != nil {
		logrus.Errorf("get conf from etcd failed, err:%v", err)
		return
	}
	fmt.Println(allConf)
	// 监控 configObj.EtcdConfig.CollectKey 对应值的变化
	go etcd.WatchConf(collectKey)
	// 2.	根据配置中的日志路径使用tail去收集日志
	err = tailfile.Init(allConf) // 把从etcd中获取的配置项加载到Init中
	if err != nil {
		logrus.Error("init tailfile failed, err: ", err)
		return
	}
	logrus.Info("init tailfile success!")
	run()
}
