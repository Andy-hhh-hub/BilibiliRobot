# B站直播机器人(BilibiliRobot)

我之前用树莓派搭建了一个监控我家狗的B站直播，https://live.bilibili.com/21172572

我几年前做过一个微信机器人，接入图灵机器人的API，现在将它接入到Ｂ站直播弹幕中，用来消遣消遣，纯粹玩玩。

### 依赖

* Python 3.5+
* pip3 install aiohttp

### 快速开始

在 var_set.py 中配置参数．

在命令行中，

```python
python bilibiliClient.py
``` 

### TODO

 - 图灵机器人的API免费版，一个机器人只能调用１００次，一共可以添加５个机器人，切换appkey来实现增加调用次数，即当一个API次数限制用完以后，自动切换到另外一个API
 - 由于摄像头视角比较小，不能完全覆盖到整个狗笼，因此，需要通过图像识别狗的位置，让摄像头自动跟踪狗的位置．使用２个舵机，用solidworks画一个符合要求的舵机云台．