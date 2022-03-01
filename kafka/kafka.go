package kafka

// 临时设置一条gopath用来导入包，在命令行添加:
// export GOPATH=/usr/local/src/go_path
// export GOPATH=F:\LogCollectorProject
import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)

func Init(address []string, chanSize int64) error {
	// 1. 生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll            // ACK
	config.Producer.Partitioner = sarama.NewCustomPartitioner() // 分区
	config.Producer.Return.Successes = true                     // 确认

	// 2. 连接kafka
	var err error
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		// logrus.Error("kafka:producer closed, err:", err)
		back := fmt.Errorf("kafka:producer closed, err:", err)
		return back
	}
	// 3. 初始化msgChan
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	// 4. 起一个后台的goroutine从msgchan中读数据发送给kafka
	go sendMsg()
	return nil
}

// 从msgChan中读取msg，发送给kafka
func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg failed, err:", err)
				return
			}
			logrus.Infof("send msg to kafka success. pid:%v offset:%v", pid, offset)
		}
	}
}

func ToMsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
