package main

import (
	"fmt"
	"kafka"
	"strings"
	"tailfile"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"main.go/etcd"
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

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topci    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogfilePath string `ini:"logfile_path"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

func run() (err error) {
	// TailObj --> log --> Client --> kafka
	for {
		// 循环读数据
		line, ok := <-tailfile.TailObj.Lines //chan tail.Line
		if !ok {
			logrus.Warn("tail file close reopen, filename:%s\n",
				tailfile.TailObj.Filename)
			time.Sleep(time.Second) // 读取出错等一秒
			continue
		}
		//如果是空行就跳过
		if len(strings.Trim(line.Text, "\r")) == 0 {
			continue
		}
		// 利用通道将同步的代码改为异步的
		// 把读出来的一行日志包装成kafka里面的msg类型， 丢到通道中
		msg := &sarama.ProducerMessage{}
		msg.Topic = "video_log"
		msg.Value = sarama.StringEncoder(line.Text)
		// 丢到通道中
		kafka.ToMsgChan(msg)
	}
}

func main() {
	var configObj = new(Config)
	// 0.	读配置文件 `go-ini`
	err := ini.MapTo(configObj, "./config/config.ini")
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
	allConf, err := etcd.GetConf(configObj.EtcdConfig.CollectKey)
	if err != nil {
		logrus.Errorf("get conf from etcd failed, err:%v", err)
		return
	}
	fmt.Println(allConf)
	// 2.	根据配置中的日志路径使用tail去收集日志
	err = tailfile.Init(allConf) // 把从etcd中获取的配置项加载到Init中
	if err != nil {
		logrus.Error("init tailfile failed, err: ", err)
		return
	}
	logrus.Info("init tailfile success!")
	// 3.	把日志通过Sarama发往kafka
	err = run()
	if err != nil {
		logrus.Error("run failed, err :", err)
		return
	}
}
