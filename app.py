# -*- encoding:utf-8 -*-

import json
import time
import requests
import threading
import xml.etree.ElementTree as ET
import uuid  
from flask import Flask, request, make_response

# 导入您本地的、正确的官方解密库
# (请确保 WXBizMsgCrypt.py 和 ierror.py 与此文件在同一目录)
try:
    from WXBizMsgCrypt import WXBizMsgCrypt 
except ImportError:
    print("="*50)
    print("错误：找不到 WXBizMsgCrypt.py 文件。")
    print("请确保 WXBizMsgCrypt.py 和 ierror.py 文件与 app.py 在同一目录中。")
    print("="*50)
    exit(1)
except AttributeError as e:
    # (捕获导入错误，如果 WXBizMsgCrypt.py 不是最新版)
    print("="*50)
    print(f"错误：WXBizMsgCrypt.py 版本不正确或导入失败: {e}")
    print("请确保您使用的是项目提供的 (支持JSON) 版本。")
    print("="*50)
    exit(1)


# --- 1. 全局配置 ---
# (请在此处填入您的密钥)

# 1.1 企业微信机器人配置 (在机器人"API"设置页面获取)
ROBOT_CALLBACK_TOKEN = "YOUR_ROBOT_TOKEN_HERE"      
ROBOT_CALLBACK_AES_KEY = "YOUR_ROBOT_AES_KEY_HERE" 

# 1.2 Coze (扣子) 配置 (在 Coze 平台 "API" 设置页面获取)
COZE_API_KEY = "pat_YOUR_COZE_API_KEY_HERE"
COZE_BOT_ID = "YOUR_COZE_BOT_ID_HERE" 
COZE_API_BASE = "https://api.coze.cn/v3" # Coze API 地址，国内版无需修改


# --- 2. Coze 流式架构 ---

# 全局字典，用于存储正在进行的 Coze 流生成器
# 结构: { "stream_id": {"iterator": gen, "buffer": "", "lock": RLock, "last_access": time, "finished": False} }
GLOBAL_COZE_STREAMS = {}
STREAM_LOCK = threading.RLock() # 用于安全地读写 GLOBAL_COZE_STREAMS
# 流的超时时间（秒），5分钟
STREAM_TIMEOUT_SECONDS = 300 

# --- 简单的生成器 (批处理移至主循环) ---
def call_coze_iterator(user_id, query):
    """
    这是一个*简单*生成器
    它会 *yield* (产生) Coze 返回的 *每一个* 内容块 (chunk)
    """
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{current_time}] [Coze生成器] 正在为 {user_id} 调用 Coze API...")
    
    try:
        headers = {
            "Authorization": f"Bearer {COZE_API_KEY}", 
            "Content-Type": "application/json",
            "Accept": "text/event-stream"
        }
        payload = {
            "bot_id": COZE_BOT_ID,
            "user_id": user_id,
            "stream": True, 
            "auto_save_history": True,
            "additional_messages": [
                {
                    "role": "user",
                    "content": query,
                    "content_type": "text"
                }
            ]
        }
        
        chat_url = f"{COZE_API_BASE}/chat"
        response = requests.post(chat_url, headers=headers, json=payload, timeout=60, stream=True)
        
        print(f"[{current_time}] [Coze生成器] API 响应: {response.status_code}")
        
        if response.status_code != 200:
            raise Exception(f"Chat API 请求失败: {response.status_code} {response.text}")

        current_event = None
        
        for line in response.iter_lines():
            if not line:
                current_event = None
                continue

            line_str = line.decode('utf-8')
            
            if line_str == "[DONE]":
                break 

            if line_str.startswith("event:"):
                current_event = line_str[len("event:"):].strip()
                continue

            if line_str.startswith("data:"):
                json_data_str = line_str[len("data:"):]
                
                if not current_event:
                    continue
                    
                try:
                    message = json.loads(json_data_str)
                    
                    if current_event == "conversation.message.delta":
                        if message.get('role') == 'assistant' and message.get('type') == 'answer':
                            content_chunk = message.get('content', '')
                            # --- 直接 yield 每一个块 ---
                            if content_chunk:
                                # (日志太频繁，暂时关闭)
                                # log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                                # print(f"[{log_time}] [Coze生成器] yield (块): {content_chunk}")
                                yield content_chunk # <--- 产生内容块
                    
                    elif current_event == "error":
                        log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                        print(f"[{log_time}] [Coze生成器] 流中错误: {message}")
                        raise Exception(f"Coze 流中错误: {message.get('msg')}")

                except json.JSONDecodeError:
                    pass
                
                current_event = None
        
        log_time = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}] [Coze生成器] {user_id} 的流正常结束。")

    except Exception as e:
        log_time = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}] [Coze生成器] 任务异常: {e}")
        yield f"[机器人处理异常: {e}]"


def start_coze_stream(user_id, query, stream_id):
    """
    (后台线程) 
    创建 Coze 生成器并将其放入全局字典中，准备好被拉取。
    """
    log_time = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{log_time}] [后台线程] 启动: 正在为 {stream_id} 创建 Coze 迭代器...")
    
    try:
        iterator = call_coze_iterator(user_id, query)
        
        with STREAM_LOCK:
            GLOBAL_COZE_STREAMS[stream_id] = {
                "iterator": iterator,
                "buffer": "", # 缓存已发送的所有内容
                "lock": threading.RLock(), # 确保同一时间只有一个 "刷新" 在拉取
                "last_access": time.time(),
                "finished": False # <-- 新增：状态标记
            }
        
        log_time = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}] [后台线程] 完成: {stream_id} 已准备就绪。")
        
    except Exception as e:
        log_time = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}] [后台线程] 失败: {stream_id} 创建失败: {e}")
        # (如果创建失败，下次刷新时会报错)

# --- 流清理线程 ---
def cleanup_expired_streams():
    """
    (后台线程) 每60秒清理一次 GLOBAL_COZE_STREAMS 中超时的流。
    """
    while True:
        try:
            time.sleep(60) # 每60秒检查一次
            
            now = time.time()
            expired_streams = []
            
            # 1. 查找所有超时的流
            with STREAM_LOCK:
                for stream_id, info in GLOBAL_COZE_STREAMS.items():
                    if (now - info["last_access"]) > STREAM_TIMEOUT_SECONDS:
                        expired_streams.append(stream_id)
            
            # 2. 删除超时的流
            if expired_streams:
                log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"[{log_time}] [清理线程] 正在清理 {len(expired_streams)} 个超时流: {expired_streams}")
                with STREAM_LOCK:
                    for stream_id in expired_streams:
                        if stream_id in GLOBAL_COZE_STREAMS:
                            # 尝试获取锁，以防万一
                            # 修复：只在流*未*锁定时才尝试获取和删除
                            if GLOBAL_COZE_STREAMS[stream_id]["lock"].acquire(blocking=False):
                                try:
                                    del GLOBAL_COZE_STREAMS[stream_id]
                                finally:
                                    GLOBAL_COZE_STREAMS[stream_id]["lock"].release() # 立即释放
                            else:
                                print(f"[{log_time}] [清理线程] 警告: 无法锁定超时的流 {stream_id}，下次再试")
                                
        except Exception as e:
            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] [清理线程] 发生异常: {e}")
# --- 清理线程结束 ---


# --- 4. 初始化 Flask 和 解密器 ---
app = Flask(__name__)

print(f"""
--- 初始化 [crypt_robot] (机器人API) ---
    Token: {ROBOT_CALLBACK_TOKEN[:4]}...
    AESKey: {ROBOT_CALLBACK_AES_KEY[:4]}...
    ReceiveId: "" (空字符串)
""")
crypt_robot = WXBizMsgCrypt(ROBOT_CALLBACK_TOKEN, ROBOT_CALLBACK_AES_KEY, "")


# --- 5. 核心：机器人API回调接口 ---
@app.route("/robot-callback", methods=["GET", "POST"])
def robot_app_hook():
    
    # --- A. GET请求：(用于URL验证) ---
    if request.method == "GET":
        try:
            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] /robot-callback (机器人) 收到 GET 验证请求...")
            msg_signature = request.args.get('msg_signature')
            timestamp = request.args.get('timestamp')
            nonce = request.args.get('nonce')
            echostr = request.args.get('echostr')
            ret, decrypted_echostr = crypt_robot.VerifyURL(msg_signature, timestamp, nonce, echostr)
            
            log_time = time.strftime('%Y-%m-%d %H:%M:%S') # 再次获取时间
            if ret == 0:
                print(f"[{log_time}] /robot-callback (机器人) URL 验证成功!")
                return make_response(decrypted_echostr, 200)
            else:
                print(f"[{log_time}] /robot-callback (机器人) URL 验证失败, ret: {ret}")
                return f"Robot Verification Failed, ret: {ret}", 200
        except Exception as e:
            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] /robot-callback (机器人) GET 异常: {e}")
            return "Internal Error", 500

    # --- B. POST请求：(流式架构) ---
    if request.method == "POST":
        
        # 定义 sReplyMsg 
        sReplyMsg = None
        # 定义 stream_id 以便在 except 中使用
        stream_id_for_error = "UNKNOWN"
            
        try:
            # 1. 从 URL 获取签名
            msg_signature = request.args.get('msg_signature')
            timestamp = request.args.get('timestamp')
            nonce = request.args.get('nonce')

            # 2. 接收原始数据 (是 JSON)
            raw_data = request.data.decode('utf-8')
            data = json.loads(raw_data)

            # 3. 提取 "encrypt" 字段
            encrypt_msg = data.get('encrypt')
            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
            if not encrypt_msg:
                print(f"[{log_time}] /robot-callback (机器人) POST 错误：未找到 'encrypt' 字段")
                return "ok", 200 

            # 4. 手动包装 XML (因为解密库是基于XML格式的)
            sPostData_xml = f"<xml><Encrypt><![CDATA[{encrypt_msg}]]></Encrypt></xml>"

            # 5. 使用 "crypt_robot" 解密
            ret, decrypted_payload_bytes = crypt_robot.DecryptMsg(sPostData_xml, msg_signature, timestamp, nonce)
            if ret != 0:
                log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"[{log_time}] /robot-callback (机器人) POST 解密失败, ret: {ret}")
                return "ok", 200 
            
            # 6. 将解密后的 bytes 作为 JSON 解析
            decrypted_json_str = decrypted_payload_bytes.decode('utf-8')
            
            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] /robot-callback (机器人) 解密的JSON: {decrypted_json_str}")
            
            data = json.loads(decrypted_json_str) 
            
            # 7. 检查 msgtype (text 或 stream)
            msg_type = data.get("msgtype")
            
            # --- 场景 A: 收到用户的新消息 ---
            if msg_type == "text":
                log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"[{log_time}] [场景 A] 收到新 'text' 消息")
                
                user_id = data.get("from", {}).get("userid")
                aibotid = data.get("aibotid") 
                query = data.get("text", {}).get("content")
                
                if not (user_id and aibotid and query):
                     print(f"[{log_time}] /robot-callback (机器人) 未能从JSON中提取 userid/query/aibotid")
                     return "ok", 200
                
                # 1. 生成新的 Stream ID
                stream_id = f"coze_stream_{uuid.uuid4()}"
                stream_id_for_error = stream_id # 记录ID，以便出错时返回
                print(f"[{log_time}] /robot-callback (机器人) 生成新 ID: {stream_id}")

                # 2. 启动后台线程来 *创建* Coze 迭代器
                thread = threading.Thread(
                    target=start_coze_stream, 
                    args=(user_id, query, stream_id)
                )
                thread.start()
                
                # 3. 立即回复第一个 "stream" 块 (符合官方文档)
                # 添加 ensure_ascii=False
                sReplyMsg = json.dumps({
                    "msgtype": "stream",
                    "stream": {
                        "id": stream_id,
                        "finish": False,
                        "content": "思考中..." # <--- 第一块回复
                    }
                }, ensure_ascii=False) 

            # --- 场景 B: 收到企微的 "流式消息刷新" ---
            elif msg_type == "stream":
                log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"[{log_time}] [场景 B] 收到 'stream' 刷新请求")
                
                stream_id = data.get("stream", {}).get("id")
                stream_id_for_error = stream_id
                
                if not stream_id:
                     print(f"[{log_time}] 'stream' 刷新请求中未找到 ID")
                     return "ok", 200
                
                stream_info = None
                with STREAM_LOCK:
                    if stream_id in GLOBAL_COZE_STREAMS:
                        stream_info = GLOBAL_COZE_STREAMS[stream_id]
                
                if not stream_info:
                    print(f"[{log_time}] 错误: 收到未知的 stream_id: {stream_id} (可能已过期或创建失败)")
                    # 添加 ensure_ascii=False
                    sReplyMsg = json.dumps({
                        "msgtype": "stream",
                        "stream": {
                            "id": stream_id,
                            "finish": True, # 结束这个无效的会话
                            "content": "[会话已过期或不存在]"
                        }
                    }, ensure_ascii=False) 
                
                else:
                    # 修复：首先检查流是否已完成
                    stream_info["last_access"] = time.time() # 更新访问时间
                    
                    if stream_info.get("finished", False):
                        log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                        print(f"[{log_time}] [僵尸请求处理器] 流 {stream_id} 已完成。重新发送最终回复。")
                        
                        sReplyMsg = json.dumps({
                            "msgtype": "stream",
                            "stream": {
                                "id": stream_id,
                                "finish": True, # <-- 发送 True
                                "content": stream_info["buffer"] # <-- 发送最终的完整缓冲区
                            }
                        }, ensure_ascii=False)
                        # (跳过下面的锁和循环，直接进入 "统一回复" 块)
                    
                    # 流未完成，继续正常逻辑
                    else:
                        # 修复：使用非阻塞锁 (blocking=False) 避免请求堆积
                        if not stream_info["lock"].acquire(blocking=False):
                            # 锁被占用：说明有*另*一个刷新请求正在处理
                            # 这是一个重复/陈旧的请求，立即丢弃
                            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                            print(f"[{log_time}] [主动丢弃] 另一个请求正在处理 {stream_id}，丢弃此次刷新")
                            return "ok", 200 # 必须回复 "ok"
                        
                        # 成功获取锁，我们是唯一的处理者
                        try:
                            # --- 主动保持 4 秒 来拉取数据 ---
                            start_time = time.time()
                            # 企微有 5 秒超时，我们用 4 秒来拉取
                            MAX_HOLD_SECONDS = 4.0 
                            current_batch_content = ""
                            finish = False
                            
                            while (time.time() - start_time) < MAX_HOLD_SECONDS:
                                try:
                                    # 1. 从生成器拉取下一块
                                    next_chunk = next(stream_info["iterator"])
                                    current_batch_content += next_chunk
                                
                                except StopIteration:
                                    # 2. Coze 流已正常结束
                                    log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                                    print(f"[{log_time}] [主动保持] Coze 流 {stream_id} 已完成。")
                                    finish = True
                                    break # 退出 while 循环
                                
                                except Exception as e:
                                    # 3. Coze 流在拉取时出错
                                    log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                                    print(f"[{log_time}] [主动保持] Coze 流 {stream_id} 发生异常: {e}")
                                    current_batch_content += f"\n[机器人出错: {e}]"
                                    finish = True
                                    break # 退出 while 循环
                            
                            # 4. 4秒钟超时，或 Coze 已完成
                            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                            print(f"[{log_time}] [主动保持] 本次拉取结束。拉取到 {len(current_batch_content)} 字符。 Finish={finish}")
                            
                            # 5. 附加到全局缓冲区 (企微要求回复 *完整* 内容)
                            stream_info["buffer"] += current_batch_content
                            
                            # 6. 构造回复
                            sReplyMsg = json.dumps({
                                "msgtype": "stream",
                                "stream": {
                                    "id": stream_id,
                                    "finish": finish,
                                    "content": stream_info["buffer"] # <--- 回复完整缓冲区
                                }
                            }, ensure_ascii=False) 
                            
                            # 7. 修复：如果流结束，*标记*为完成，而不是删除
                            if finish:
                                print(f"[{log_time}] [主动保持] 标记流为已完成: {stream_id}")
                                stream_info["finished"] = True # <-- 标记
                        
                        finally:
                            # 无论如何，释放锁
                            stream_info["lock"].release()
            
            # --- 场景 C: 未知消息类型 ---
            else:
                log_time = time.strftime('%Y-%m-%d %H:%M:%S') 
                print(f"[{log_time}] 收到未处理的 msgtype: {msg_type}")
                return "ok", 200

            # --- 统一回复 ---
            # (sReplyMsg 必须在 场景 A 或 B 中被赋值)
            if not sReplyMsg:
                 log_time = time.strftime('%Y-%m-%d %H:%M:%S') 
                 print(f"[{log_time}] 逻辑错误：sReplyMsg 未被赋值。")
                 return "ok", 200
                 
            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
            # 打印时解码（如果需要）
            try:
                print_msg = sReplyMsg.decode('utf-8') if isinstance(sReplyMsg, bytes) else sReplyMsg
            except:
                print_msg = sReplyMsg
            print(f"[{log_time}] /robot-callback (机器人) 构造明文回复: {print_msg}")

            ret, encrypted_reply_json = crypt_robot.EncryptMsgJSON(sReplyMsg, nonce, timestamp)
            
            if ret != 0:
                log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"[{log_time}] /robot-callback (机器人) *回复* 加密失败, ret: {ret}")
                return "ok", 200

            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] /robot-callback (机器人) 正在返回 *加密 JSON* 回复 (Content-Type: application/json)...")
            
            # 明确设置 Content-Type 为 application/json
            response = make_response(encrypted_reply_json, 200)
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
            return response

        except Exception as e:
            # 发生顶级异常，尝试以错误信息结束流
            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] /robot-callback (机器人) POST 顶层异常: {e}")
            
            try:
                # 尝试告诉企微 "这个流失败了"
                # 添加 ensure_ascii=False
                sReplyMsg = json.dumps({
                    "msgtype": "stream",
                    "stream": {
                        "id": stream_id_for_error,
                        "finish": True,
                        "content": f"[服务器顶层异常: {e}]"
                    }
                }, ensure_ascii=False) 
                
                # 再次获取签名 (注意: 如果 nonce/timestamp 丢失，这里也会失败)
                msg_signature = request.args.get('msg_signature')
                timestamp = request.args.get('timestamp')
                nonce = request.args.get('nonce')
                ret, encrypted_reply_json = crypt_robot.EncryptMsgJSON(sReplyMsg, nonce, timestamp)
                if ret == 0:
                    # (异常处理中) 明确设置 Content-Type
                    response = make_response(encrypted_reply_json, 200)
                    response.headers['Content-Type'] = 'application/json; charset=utf-8'
                    return response
                else:
                    return "ok", 200 # 放弃
            except:
                return "ok", 200 # 放弃


# --- 8. 启动服务 ---
if __name__ == "__main__":
    # 启动清理线程
    janitor_thread = threading.Thread(target=cleanup_expired_streams, daemon=True)
    janitor_thread.start()
    
    print("--- 启动 Flask 服务 ---")
    print("请确保:")
    print(" 1. WXBizMsgCrypt.py 和 ierror.py 在此目录中")
    print(" 2. 已运行 'pip install pycryptodome requests flask'")
    print(" 3. 已在 app.py 中填写 ROBOT_ 和 COZE_ 的密钥")
    print("="*50)
    app.run(host='0.0.0.0', port=5000, debug=False)