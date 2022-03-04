package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"main.go/common"
	"main.go/tailfile"
)

// etcd是使用go语言开发的分布式key-value存储系统
// 常见应用场景有三类————
// 1、接口相应（即，在同一个分布式集群中的进程或服务可通过etcd找到彼此并建立连接
// 2、热输入（类似热插拔，即etcd在节点注册一个watcher，利用watcher，在每次配置有更新时，实现实时更新
// 3、分布式锁（etcd使用了raft算法保持数据的强一致性，可以利用该特点实现分布式锁
// 		锁服务有两种方式，保持独占和控制时序
// 在当前项目中使用etcd的目的主要是出于原因`2`，日志的记录不好出现中断，故热输入
// 这个功能在有新的日志记录项需要添加时就显得相当方便
// ps:在当前的v2.3版本中，会另外通过程序向etcd中传入一个json格式的k-v对，其中
// 		包含有日志文件的路径信息以及topic，tail当前疑似通过该k-v对来获取地址，
// 		main函数中的`CollectConfig`结构体中的地址已经弃用了。
// 		也就是说，在不关闭服务的情况下更新日志目录是通过etcd实现的，
// 		它与tail协同实现了日志文件的热输入
// pps:etcd实时获取更新的配置文件是通过键值对实现的，即另外向etcd服务器中发送配置文件的键值对，etcd会
//		实时监控键值对并发现键值对有了更改，并以此增加或删除tail的监控项
// 		理论上这个过程其实也可以用tail来实现？比如专门设置一个配置更改文件？
// 		好像又不行,tail的读取好像不太能读删除,后续可以考虑一下

// 配置文件在etcd中以key-value格式存储，value存储的是json格式的数据，一个json中可填入多组配置文件地址和topic

// etcd 相关操作

var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		err = fmt.Errorf("connect to etcd failed, err:%v", err)
		return
	}
	return
}

// 拉取日志收集配置项的函数
func GetConf(key string) (collectEntryList []common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get conf from etcd by key:%s failed, err:%v", key, err)
		return
	}
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len:0 conf from etcd by key:%s", key)
		return
	}
	ret := resp.Kvs[0]
	// ret.Value json格式字符串
	err = json.Unmarshal(ret.Value, &collectEntryList)
	if err != nil {
		logrus.Errorf("json unmarshal failed, err:%v", err)
		return
	}
	return
}

// 监控日志搜集项配置变化的函数
func WatchConf(key string) {
	for {
		watchCh := client.Watch(context.Background(), key)
		for wresp := range watchCh {
			logrus.Info("detected new conf from etcd!")
			for _, evt := range wresp.Events {
				fmt.Printf("type:%s key:%s value:%s\n", evt.Type, evt.Kv.Key, evt.Kv.Value)
				var newConf []common.CollectEntry
				// 原代码为：if evt.Type == clientv3.EventTypeDelete，该函数已经被取消，只能另找方法表示DELETE时间
				// 查阅文档，找到对应结构体，delete对应的int32数为1，更改代码后如下：
				// const ( PUT    Event_EventType = 0 DELETE Event_EventType = 1 EXPIRE Event_EventType = 2 )
				if evt.Type == 1 {
					// 如果是删除（1对应的就是delete
					logrus.Warning("etcd delete the key!")
					tailfile.SendNewConf(newConf) // 没有人接受就是阻塞的
					continue
				}
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					logrus.Errorf("json unmarshal new conf failed, err:%v", err)
					break
				}
				// 告诉tailfile模块该启用新配置了！
				tailfile.SendNewConf(newConf) // 没有人接受就是阻塞的
			}
		}
	}
}
