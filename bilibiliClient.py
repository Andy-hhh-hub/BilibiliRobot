from bilibili_api import Credential, Danmaku, sync
from bilibili_api.live import LiveDanmaku, LiveRoom
import os
from handlers.current_chatbot import CurrentChatBotClient
from utils.audio_reader import audioReader, SentenceSplitter
import json
import queue
import threading
import subprocess

bilibili_config_path = os.environ.get("bilibili_config_path",
                                      "/Users/lidanfeng/PycharmProjects/git_PJ/BilibiliRobot/configs/bilibili_config.json")
bilibili_config = json.load(open(bilibili_config_path))
# 初始化聊天机器人
stream = True
chatbot = CurrentChatBotClient('moonshot', stream=stream)

global audio_generate_flag, data_queue  # 只允许生成一条语音播报，以防全部弹幕都回答
audio_generate_flag = True
# 定义一个等待转语音队列
data_queue = queue.Queue()


def read_text(text):
    # punctuation = ['.', '?', '!', '，', '。', '；', '？', '！', '：', '……', '、', ',', ';', '?']
    # for char in punctuation:
    #     text = text.replace(char, '')
    # 创建一个临时文件，将文本写入
    with open("temp.txt", "w", encoding="utf-8") as f:
        f.write(text)
    # 使用系统调用朗读文件内容
    cmd = f"say -f temp.txt"
    subprocess.call(cmd, shell=True)
    # 删除临时文件
    os.remove("temp.txt")


# 定义一个线程函数，不断打印队列里的元素
def real_time_generate_audio():
    """ 实时生成语音播报 """
    global audio_generate_flag, data_queue
    while not audio_generate_flag:
        item = data_queue.get()
        print(f"Got item: {item}")
        # ======== mac 系统合成语音 ========
        read_text(item)
        # ======== mac 系统合成语音 ========
        data_queue.task_done()
    # 跳出来时清空队列
    data_queue.queue.clear()


def test_audio():
    global audio_generate_flag,data_queue

    msg = "介绍一下你自己"
    if audio_generate_flag:
        audio_generate_flag = False
        try:
            # 创建一个实时转语音线程
            print_thread = threading.Thread(target=real_time_generate_audio)
            print_thread.daemon = True
            print_thread.start()
            dm_str = ''
            messages = [
                {
                    "role": "system",
                    "content": "你是(Angela正能量直播间)的主播,你叫Angela,你是一个正能量的人生导师主播，\
                    你会各种加油打气和输出正能量反馈的谈话技巧，你也会为用户提供安全，有帮助，准确的回答。\
                    你的回答需要尽量精简，控制在5句话以内。",
                },
                {"role": "user", "content": msg},
            ]
            sentence_generate = SentenceSplitter().getSentent(chatbot.get_chatbot_response(messages))
            dm_str = ''
            temp_sentence = ''
            for i, sentence in enumerate(sentence_generate):
                dm_str += sentence
                temp_sentence += sentence
                if i % 8 == 0 and i != 0:
                    data_queue.put(temp_sentence)  # 单句单句生成
                    temp_sentence = ''
            if temp_sentence != '':
                data_queue.put(temp_sentence)  # 加入最后一句
            # 发送弹幕
            print("====== final dm_str:", dm_str)
            # data_queue.put(dm_str)  # 整句生成
            # 等待队列中的所有数据被处理
            data_queue.join()
            # 语音合成结束
            audio_generate_flag = True
        except Exception as e:
            print(e)
            audio_generate_flag = True


# 自己直播间号
ROOMID = bilibili_config["bilibili"]["roomId"]
# 凭证 根据回复弹幕的账号填写
credential = Credential(
    sessdata=bilibili_config["cookies"]["sessdata"],
    bili_jct=bilibili_config["cookies"]["bili_jct"],
    buvid3=bilibili_config["cookies"]["buvid3"],
)
# 监听直播间弹幕
monitor = LiveDanmaku(ROOMID, credential=credential)
# 用来发送弹幕
sender = LiveRoom(ROOMID, credential=credential)
# 自己的UID 可以手动填写也可以根据直播间号获取
UID = sync(sender.get_room_info())["room_info"]["uid"]


@monitor.on("DANMU_MSG")
async def recv(event):
    global audio_generate_flag, data_queue
    # 发送者UID
    uid = event["data"]["info"][2][0]
    msg = event["data"]["info"][1]
    user_name = event["data"]["info"][2][1]
    # 排除自己发送的弹幕
    if uid == UID:
        return
    # 非本人弹幕文本处理
    if msg == "你好":
        # 发送弹幕
        dm_str = "{},你好呀！".format(user_name)
        await sender.send_danmaku(Danmaku(dm_str))
    elif audio_generate_flag:
        audio_generate_flag = False
        try:
            data_queue.put("这里回答一下{}的弹幕问题。".format(user_name))
            # 创建一个实时转语音线程
            print_thread = threading.Thread(target=real_time_generate_audio)
            print_thread.daemon = True
            print_thread.start()
            dm_str = ''
            messages = [
                {
                    "role": "system",
                    "content": "你是(Angela正能量直播间)的主播,你叫Angela,你是一个正能量的人生导师主播，\
                        你会各种加油打气和输出正能量反馈的谈话技巧，你也会为用户提供安全，有帮助，准确的回答。\
                        你的回答需要尽量精简，控制在5句话以内。",
                },
                {"role": "user", "content": msg},
            ]
            sentence_generate = SentenceSplitter().getSentent(chatbot.get_chatbot_response(messages))
            dm_str = ''
            temp_sentence = ''
            for i, sentence in enumerate(sentence_generate):
                dm_str += sentence
                temp_sentence += sentence
                if i % 8 == 0 and i != 0:
                    data_queue.put(temp_sentence)  # 单句单句生成
                    temp_sentence = ''
            if temp_sentence != '':
                data_queue.put(temp_sentence)  # 加入最后一句
            # 发送弹幕
            print("====== final dm_str:", dm_str)
            # data_queue.put(dm_str)  # 整句生成
            # await sender.send_danmaku(Danmaku(dm_str))
            # 等待队列中的所有数据被处理
            data_queue.join()
            # 语音合成结束
            audio_generate_flag = True
        except Exception as e:
            print(e)
            audio_generate_flag = True
    # 如果啥都没做可以直接返回
    return


# 启动监听
sync(monitor.connect())


# 测试语音播报
# test_audio()
