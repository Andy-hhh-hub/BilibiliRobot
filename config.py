import configparser
import os
import queue

# os.environ["config_path"] = "/Users/lidanfeng/PycharmProjects/git_PJ/BilibiliRobot/configs/config.ini"
# # 获取环境变量
# config_path = os.environ.get("config_path","/Users/lidanfeng/PycharmProjects/git_PJ/BilibiliRobot/configs/config.ini")
#
# config = configparser.ConfigParser()
# config.read(config_path)
# print(config["bilibili"]["roomURL"])


# 定义一个队列
data_queue = queue.Queue()
# 向队列添加元素
data_queue.put("data1")
# 强制清空队列
data_queue.queue.clear()


# import os
# import subprocess
#
# def read_text(text):
#     # 创建一个临时文件，将文本写入
#     with open("temp.txt", "w", encoding="utf-8") as f:
#         f.write(text)
#     # 使用系统调用朗读文件内容
#     cmd = f"say -f temp.txt"
#     subprocess.call(cmd, shell=True)
#     # 删除临时文件
#     os.remove("temp.txt")
#
# # 示例：朗读中英文本
# chinese_text = "你好，世界！"
# english_text = "Hello, world!"
#
# read_text(chinese_text)
# read_text(english_text)

#
print(16%8)

