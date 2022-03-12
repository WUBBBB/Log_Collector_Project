# Log_Collector_Project
从零开始为视频网站实现日志搜集项目

## LogAgent_V3.99

根据[GO语言日志收集项目](https://www.bilibili.com/video/BV1Df4y1C7o5?)

已经完美复现，且全部功能均正常

但是原项目中的细微瑕疵也得到了相应的保留，log_agent部分主要体现在etcd.WatchConf()函数，其中的删除操作只是停止了相应的tail向kafka中发送数据，tail仍在继续监视对应文件。这对系统资源造成了浪费，需要使用kill、cleanup、stop等中的某种正确方式来完全关闭相应的tail



***

- [ ] etcd.WatchConf实现

***
