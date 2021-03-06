# async-runner
async runner 异步任务队列
- 基于fastHttp的http协议, 支持多种语言的客户端, 目前只在项目中实现了go方便用户(client/client_http.go)
- 基于redis持久化任务
- 基于mysql任务日志(失败,成功) //todo
- 极少的资源占用
- 自定义重试时间

## usage

### 1. 运行服务
async-runner.exe
config.json
### 2. 编写Worker
```go
// 绑定本地9999端口以接受回调
c := client.NewHttpReceiver(":9999")
// 监听test话题
c.AddListener("test", func(data []byte) (err error) {
    log.Info("test", "run", data)
    return
})
c.StartSever()
```
### 2. 添加延时任务
```go
// 绑定本地9999端口以接受回调
c := client.NewHttpPusher("http://127.0.0.1:9989", "http://127.0.0.1:9999")
// 添加一个话题为test,延时1s执行的job,内容为[]byte{1,10}
c.Add("test", 1, []byte{1, 10})
```

顺利的话就你能在Worker里看到log: run [1,10]

代码详情请看`tests/default_test.go`

## conf

```
{
  "server_http": ":9999", // 服务器监听的地址
  "persistence": true, // 持久化到Redis
  "redis": "localhost:6379", // redis host
  "log": true, // 暂无用
  "mysql": "root:@tcp(localhost:3306)/test", // 暂无用
  "retry":[3,7,17,30] // 代表任务失败重试4次,4次间隔时间为3 7 17 30秒
}
```
