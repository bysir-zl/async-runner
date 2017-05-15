# async-runner
async runner 异步任务队列
- 基于fastHttp的http协议, 支持多种语言的客户端, 目前只在项目中实现了go方便用户(client/client_http.go)
- 基于redis持久化任务
- 基于mysql任务日志(失败,成功)
- 极少的资源占用
- 自定义重试时间

## usage
```go

```