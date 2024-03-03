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
from openai import OpenAI
import json
# 获取环境变量
bilibili_config_path = os.environ.get("bilibili_config_path", "/Users/lidanfeng/PycharmProjects/git_PJ/BilibiliRobot/configs/bilibili_config.json")
bilibili_config = json.load(open(bilibili_config_path))

class CurrentChatBotClient:

    def __init__(self, chatbot_name, stream=False):
        """
        初始化 CurrentChatBotClient。

        Args:
            chatbot_name (str): 聊天机器人的名称。
            stream (Stream): 聊天机器人是否流式输出。
        """
        self.chatbot_name = chatbot_name
        self.stream = stream
        if self.chatbot_name == "moonshot":
            self.client = OpenAI(
                api_key=bilibili_config['chatbot']['moonshot_api_key'],
                base_url=bilibili_config['chatbot']['moonshot_base_url'],
            )
        else:  # 默认客户端
            self.client = OpenAI(
                api_key=bilibili_config['chatbot']['moonshot_api_key'],
                base_url=bilibili_config['chatbot']['moonshot_base_url'],
            )

    def get_moonshot_output(self, messages):
        """
        获取 moonshot 的回答输出。流式和非流式

        Args:
            messages (list): 对话消息列表。[
                {
                    "role": "system",
                    "content": "你是 Kimi，由 Moonshot AI 提供的人工智能助手，你更擅长中文和英文的对话。你会为用户提供安全，有帮助，准确的回答。同时，你会拒绝一些涉及恐怖主义，种族歧视，黄色暴力等问题的回答。Moonshot AI 为专有名词，不可翻译成其他语言。",
                },
                {"role": "user", "content": "你好，我叫李雷，1+1等于多少？"},
            ]

        Returns:
            generator: 流式输出的生成器。
        """
        if self.stream:  # 流式输出
            response = self.client.chat.completions.create(
                model=bilibili_config['chatbot']['moonshot_model'],
                messages=messages,
                temperature=0.3,
                stream=True,
            )

            collected_messages = []
            for idx, chunk in enumerate(response):
                # print("Chunk received, value: ", chunk)
                chunk_message = chunk.choices[0].delta
                if not chunk_message.content:
                    continue
                collected_messages.append(chunk_message)  # save the message
                print(f"#{idx}: {''.join([m.content for m in collected_messages])}")
                yield chunk_message.content
            print(f"Full conversation received: {''.join([m.content for m in collected_messages])}")
        else:
            completion = self.client.chat.completions.create(
                model=bilibili_config['chatbot']['moonshot_model'],
                messages=messages,
                temperature=0.3,
            )
            print(completion.choices[0].message)
            return completion.choices[0].message

    def get_chatbot_response(self, messages):
        """获取ChatGPT的回复

        Args:
            messages (list): 对话消息列表。

        Returns:
            str: ChatGPT的回复。
        """
        if self.chatbot_name == 'moonshot':
            response = self.get_moonshot_output(messages)
            return response
        else:  # 默认输出
            response = self.get_moonshot_output(messages)
            return response


if __name__ == '__main__':
    stream = True
    chatbot = CurrentChatBotClient('moonshot', stream=stream)
    messages = [{"role": "user", "content": "你好，我叫李雷，1+1等于多少？"}, ]
    if stream:
        for idx, chunk_message in enumerate(chatbot.get_chatbot_response(messages)):
            print(f"Chunk {idx + 1}: {chunk_message.content}")
    else:
        ans_str = chatbot.get_chatbot_response(messages)
        print(ans_str)
