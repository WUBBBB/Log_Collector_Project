package tailfile

import (
	"fmt"

	"github.com/hpcloud/tail"
)

var (
	tailObj *tail.Tail
)

func Init(fileName string) (err error) {
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	//打开文件开始读取数据
	tailObj, err = tail.TailFile(fileName, config)
	if err != nil {
		// fmt.Println("tail file failed, err:", err)
		err = fmt.Errorf("tailfile: creat tailObj for path:%s failed, err:%v\n", fileName, err)
		return
	}
	return
}
