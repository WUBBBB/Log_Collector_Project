package tailfile

import (
	"github.com/sirupsen/logrus"
	"main.go/common"
)

// tailTask的管理者

type tailTaskMgr struct {
	tailTaskMap      map[string]*tailTask       // 所有的tailtask任务
	collectEntryList []common.CollectEntry      // 所有配置项
	confChan         chan []common.CollectEntry // 等待新传入的配置项的通道
}

var (
	ttMgr *tailTaskMgr
)

func Init(allConf []common.CollectEntry) (err error) {
	// allConf里存了若干个日志的收集项
	// 函数功能————针对每个日志收集项初始化一个tailObj
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry),
	}
	for _, conf := range allConf {
		// 创建一个日志收集任务
		tt := newTailTask(conf.Path, conf.Topic)
		// 打开日志文件准备读
		err = tt.Init()
		if err != nil {
			// fmt.Println("tail file failed, err:", err)
			logrus.Errorf("tailfile: creat tailObj for path:%s failed, err:%v\n", conf.Path, err)
			continue
		}
		logrus.Infof("creat a tail task for path:%s success", conf.Path)
		// 把创建的tailTask登记在册，方便后续管理
		ttMgr.tailTaskMap[tt.path] = tt
		// 起一个goroutine收集日志-Start！
		go tt.run()
	}
	// 在后台等新的配置来
	go ttMgr.watch()
	return
}

// 循环等待新配置传入confChan，有值后开始对之前的tailTask进行删除新增
func (t *tailTaskMgr) watch() {
	for {
		// 派一个小弟等着新配置来
		newConf := <-t.confChan // 取到值就说明有新配置到了
		// 新配置到了后要管理下之前启动的那些tailTask
		logrus.Infof("get new conf from etcd, conf:%v. start tailTask...", newConf)
		for _, conf := range newConf {
			// 1. 原来存在的任务不做处理
			if t.isExist(conf) {
				continue
			}
			// 2. 原来没有的tailTask要创建
			// 创建一个日志收集任务
			tt := newTailTask(conf.Path, conf.Topic)
			err := tt.Init()
			if err != nil {
				logrus.Errorf("tailfile: creat tailObj for path:%s failed, err:%v\n", conf.Path, err)
				continue
			}
			logrus.Infof("creat a tail task for path:%s success", conf.Path)
			// 把创建的tailTask登记在册，方便后续管理
			t.tailTaskMap[tt.path] = tt
			// 起一个goroutine收集日志-Start！
			go tt.run()

		}
		// 3. 原来有现在没的任务要停掉
		// 找出原来tailTaskMap中存在，但是newConf不存在的那些tailTask，停掉
		for key, task := range t.tailTaskMap {
			var isFound bool
			for _, conf := range newConf {
				if key == conf.Path {
					isFound = true
					break
				}
			}
			if !isFound {
				// 这个tail要停掉了
				logrus.Infof("the task collect path:%s need to stop", task.path)
				delete(t.tailTaskMap, key) // 从管理类中删掉
				task.cancel()
			}
		}
	}
}

// 目前存在的问题————如果logagent停了，下次启东时不是从上次的位置开始	可以参考filebeat
// 判断tailTaskMap中是否存在该收集项
func (t *tailTaskMgr) isExist(conf common.CollectEntry) bool {
	_, ok := t.tailTaskMap[conf.Path]
	return ok
}

func SendNewConf(newConf []common.CollectEntry) {
	ttMgr.confChan <- newConf
}
