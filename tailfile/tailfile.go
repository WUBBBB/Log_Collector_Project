package tailfile

import (
	"fmt"

	"github.com/hpcloud/tail"
	"main.go/common"
)

type tailTask struct {
	path    string
	topic   string
	tailObj *tail.Tail
}

func Init(allConf []common.CollectEntry) (err error) {
	// allConf里存了若干个日志的收集项
	// 针对每个日志收集项创建一个tailObj
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	for _, conf := range allConf {
		tt := tailTask{
			path:  conf.Path,
			topic: conf.Topic,
		}
		//打开文件开始读取数据
		tt.tailObj, err = tail.TailFile(tt.path, config)
		if err != nil {
			// fmt.Println("tail file failed, err:", err)
			err = fmt.Errorf("tailfile: creat TailObj for path:%s failed, err:%v\n", tt.path, err)
			return
		}
	}
	return
}
