package tailfile

import (
	"context"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"main.go/kafka"
)

type tailTask struct {
	path    string
	topic   string
	tailObj *tail.Tail
	ctx     context.Context
	cancel  context.CancelFunc
}

func newTailTask(path, topic string) *tailTask {
	ctx, cancel := context.WithCancel(context.Background())
	tt := &tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
	}
	return tt
}

// 使用tail包打开日志文件
func (t *tailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	t.tailObj, err = tail.TailFile(t.path, cfg)
	return
}

func (t *tailTask) run() {
	// 读取日志，发往kafka
	logrus.Infof("collect for path:%s is running...", t.path)
	// TailObj --> log --> Client --> kafka
	for {
		select {
		case <-t.ctx.Done():
			logrus.Infof("path:%s is closing...", t.path)
			return
		// 循环读数据
		case line, ok := <-t.tailObj.Lines: //chan tail.Line
			if !ok {
				logrus.Warn("tail file close reopen, path:%s\n", t.path)
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
			msg.Topic = t.topic
			msg.Value = sarama.StringEncoder(line.Text)
			// 丢到通道中
			kafka.ToMsgChan(msg)
		}
	}
}
