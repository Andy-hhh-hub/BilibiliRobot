#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# ====================================
# @Project ：BilibiliRobot
# @IDE     ：PyCharm
# @Author  ：Huang Andy Hong Hua
# @Email   ：
# @Date    ：2024/3/20 10:33
# ====================================

import os
import LangSegment
import simpleaudio as sa
import time
import shutil
from threading import Thread


class MyThread(Thread):  # 继承Thread类
    def __init__(self, func, args):
        # 关于super的用法可以自行百度
        super(MyThread, self).__init__()  # 初始化Thread
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        # threading.Thread.join(self)
        try:
            return self.result
        except Exception:
            return None


def copyFile(sourcePath, targetPath):
    shutil.copy(sourcePath, targetPath)
    print('copy:' + sourcePath + ' to ' + targetPath)


# 建一个audioReader类，用于播放某个文件夹里的文件
class audioReader:
    def __init__(self, folderPath):
        self.folderPath = folderPath

    def convertDate(self, str):
        try:
            res = time.strptime(str, "%Y-%m-%d-%H-%M-%S")
            return res
        except ValueError:
            return False

    def play(self, fileName):
        # 播放声音
        audio_file_name = os.path.join(self.folderPath, fileName)
        wave_obj = sa.WaveObject.from_wave_file(audio_file_name)
        play_obj = wave_obj.play()
        play_obj.wait_done()  # 等到声音播放完毕
        # 播完后删除文件
        os.remove(audio_file_name)
        print('done and remove:' + audio_file_name)

    def getFileName(self):
        file_name = ''
        file_list = os.listdir(self.folderPath)
        # 如果插播最紧急的.wav,则播放
        if 'urgent.wav' in file_list:
            return 'urgent.wav'
        # 检查文件夹里的声音文件，挑取文件名是日期且最小的返回文件名
        for file in file_list:
            time_str = file.split('.')[0]
            time_obj = self.convertDate(time_str)
            if time_obj:
                if file_name == '' or time_obj < min_time_obj:
                    file_name = file
                    min_time_obj = time_obj
        return file_name

    # 一直跑，取self.folderPath里名字最小的文件去播放
    def alwaysRun(self):
        while True:
            fileName = self.getFileName()
            if fileName:
                self.play(fileName)
            else:
                time.sleep(1)

    def run(self):
        thread_list = []
        always_run = MyThread(func=self.alwaysRun, args=())
        thread_list.append(always_run)
        always_run.start()
        print("audioReader is running...", self.folderPath)


# 语句按标点切分器
class SentenceSplitter:
    def __init__(self):
        self.punctuation = ['.', '?', '!', '，', '。', '；', '？', '！', '：', '……', '、', ',', ';', '?']

    def getTextSentence(self, text):
        sentences = []
        start = 0
        for i, char in enumerate(text):
            if char in self.punctuation:
                sentences.append(text[start:i + 1])
                start = i + 1
                yield text[start:i + 1]
        if start < len(text):
            sentences.append(text[start:])
            yield text[start:]

    def getGeneratorSentence(self, generator):
        text = ''
        for i, char in enumerate(generator):
            text += char
            if char in self.punctuation:
                yield text
                text = ''
        if len(text):
            yield text

    def getSentent(self, input):
        if isinstance(input, str):
            return self.getTextSentence(input)
        else:
            return self.getGeneratorSentence(input)


if __name__ == '__main__':
    # 复制某个文件到另一个路径
    sourcePath = '/Users/lidanfeng/PycharmProjects/git_PJ/GPT-SoVITS/generated_audio-S2G488k.wav'
    targetPath = '/Users/lidanfeng/PycharmProjects/git_PJ/BilibiliRobot/audio_reader_files/' \
                 + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime()) + '.wav'
    copyFile(sourcePath, targetPath)
    time.sleep(3)
    sourcePath = '/Users/lidanfeng/PycharmProjects/git_PJ/GPT-SoVITS/welcome_audio.wav'
    targetPath = '/Users/lidanfeng/PycharmProjects/git_PJ/BilibiliRobot/audio_reader_files/' \
                 + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime()) + '.wav'
    copyFile(sourcePath, targetPath)

    folderPath = '/Users/lidanfeng/PycharmProjects/git_PJ/BilibiliRobot/audio_reader_files'
    player = audioReader(folderPath)
    player.run()
