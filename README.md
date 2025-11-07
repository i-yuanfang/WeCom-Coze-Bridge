# 企业微信机器人 Coze 桥接项目 (WeCom-Coze-Bridge)

这是一个用于将 [企业微信群机器人](https://developer.work.weixin.qq.com/document/path/91770) 与 [Coze (扣子)](https://www.coze.cn/) 智能体 API 对接的 Python Flask 应用。

本项目实现了企业微信机器人的流式消息响应（Stream）协议，允许 Coze Bot 的回答以打字机的方式实时显示在企业微信中。

## 主要功能

* **流式响应**: 完全支持企业微信机器人的 `stream` 消息类型。
* **Coze 对接**: 集成 Coze API (`/v3/chat`)，支持流式获取回复。
* **会话管理**: 自动管理和超时清理会话流，确保服务稳定。
* **加解密**: 包含企业微信官方（修改版）的加解密库，支持机器人 API 所需的 JSON 格式加密。

## 项目结构.
├── app.py # 主应用服务 (Flask)  
├── WXBizMsgCrypt.py # 企业微信消息加解密库 (已修改支持 JSON 加密)  
├── ierror.py # 加解密库的错误码  
└── requirements.txt # Python 依赖

## 部署与使用指南

### 1. 先决条件

* Python 3.7+
* 一个可以从公网访问的服务器（用于接收企业微信的回调）
* 一个已创建的企业微信机器人（并获取了 `Token` 和 `EncodingAESKey`）
* 一个已创建的 Coze Bot（并获取了 `API Key` 和 `Bot ID`）

### 2. 安装

1.  克隆本项目到您的服务器：
    ```bash
    git clone https://github.com/i-yuanfang/WeCom-Coze-Bridge.git
    cd wecom_coze_bridge
    ```

2.  安装 Python 依赖：
    ```bash
    pip install -r requirements.txt
    ```
    (主要包含: `flask`, `requests`, `pycryptodome`)

### 3. 配置

打开 `app.py` 文件，在顶部的 `--- 1. 全局配置 ---` 部分填入您的密钥信息：

```python
# --- 1. 全局配置 ---

# 1.1 企业微信机器人配置 (在机器人"API"设置页面获取)
ROBOT_CALLBACK_TOKEN = "YOUR_ROBOT_TOKEN_HERE"      
ROBOT_CALLBACK_AES_KEY = "YOUR_ROBOT_AES_KEY_HERE" 

# 1.2 Coze (扣子) 配置 (在 Coze 平台 "API" 设置页面获取)
COZE_API_KEY = "pat_YOUR_COZE_API_KEY_HERE"
COZE_BOT_ID = "YOUR_COZE_BOT_ID_HERE" 
COZE_API_BASE = "[https://api.coze.cn/v3](https://api.coze.cn/v3)" # Coze API 地址，国内版无需修改
```

## 4. 运行服务
配置完成后，在服务器上运行 Flask 应用：

```Bash
python app.py
```
服务默认启动在 0.0.0.0:5000 端口。 （推荐使用 gunicorn 或其他 WSGI 服务器在生产环境中运行）


生产环境运行示例 (例如使用 gunicorn)
 ```bash
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

## 5. 配置企业微信机器人
确保您的服务已通过公网 IP 或域名暴露在 5000 端口。

进入企业微信机器人的管理后台，在 API 设置页面：

回调URL: 填入您的服务地址，必须包含路径 /robot-callback。

例如: http://your-public-domain.com:5000/robot-callback

Token: 填入您在 app.py 中配置的 ROBOT_CALLBACK_TOKEN。

EncodingAESKey: 填入您在 app.py 中配置的 ROBOT_CALLBACK_AES_KEY。

点击“保存”。企业微信会向您的回调 URL 发送一个 GET 请求以验证服务。

查看 app.py 运行日志，如果看到 [/robot-callback (机器人) URL 验证成功!] 字样，说明配置成功。

将机器人添加到企业微信群聊中，@机器人 并向它提问，即可看到来自 Coze 的流式回复。

注意事项
网络访问: 您的服务器必须能访问 Coze API (api.coze.cn)，同时企业微信服务器也必须能访问您的回调 URL。

超时: 企业微信的“流式消息刷新”请求有 5 秒超时限制。本项目通过 MAX_HOLD_SECONDS = 4.0 确保在此时间内拉取 Coze 数据并返回，避免超时。

会话清理: 项目内置了 cleanup_expired_streams 线程，自动清理 5 分钟（300秒）未活动的 Coze 会话，防止内存泄漏。
