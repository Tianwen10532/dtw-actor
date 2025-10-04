import socket
import threading
import json
import uuid
from redis import Redis, ConnectionError
import time

def bind_redis(self, redis_conf):
    self.redis_ip = redis_conf['host']
    self.redis_port = redis_conf['port']

# 在指定端口上启动服务
def serve(self, port):
    host = '0.0.0.0'
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(5)
        
        while True:
            conn, addr = s.accept()
            client_thread = threading.Thread(target=self.handle_client, args=(conn, addr))
            client_thread.daemon = True
            client_thread.start()


def handle_client(self, conn, addr):
    try:
        data = conn.recv(1024).decode().strip()
        if not data:
            return
        
        try:
            # 解析JSON请求
            req = json.loads(data)
            input_host = req.get('redis_host')
            input_port = req.get('redis_port')
            input_key = req.get('redis_key')
            method_name = req.get('method_name')

            # 验证请求参数
            if not all([input_host, input_port, input_key,method_name]):
                error = "Invalid request: missing parameters"
                conn.sendall(json.dumps({"error": error}).encode())
                print(error)
                return
            # 连接输入Redis
            with Redis(host=input_host, port=input_port, db=0, socket_timeout=5) as r_in:
                # 读取原始值
                value=None
                while(value is None):
                    value = r_in.get(input_key)
                    time.sleep(0.5)
            # 准备响应（返回另一个Redis信息）
            response = {
                "redis_host": self.redis_ip,
                "redis_port": self.redis_port,
                "redis_key": f'{uuid.uuid4()}'  # 生成唯一键名
            }
            conn.sendall(json.dumps(response).encode())

            # 启动异步工作线程
            worker_thread = threading.Thread(
                target=self.redis_worker,
                args=(
                    {"host": input_host, "port": input_port}, method_name,
                    input_key,
                    response["redis_key"]
                )
            )
            worker_thread.daemon = True
            worker_thread.start()
        except json.JSONDecodeError:
            error = "Invalid JSON format"
            conn.sendall(json.dumps({"error": error}).encode())
    
    except Exception as e:
        print(f"Client handling error: {str(e)}")
    finally:
        conn.close()

def redis_worker(self,input_redis_conf, method_name, input_key, output_key):
    try:
        # 连接输入Redis
        with Redis(host=input_redis_conf['host'], port=input_redis_conf['port'], db=0, socket_timeout=5) as r_in:
            # 读取原始值
            value=None
            while(value is None):
                value = r_in.get(input_key)
                time.sleep(0.5)
            
            input:str = value.decode()
            method=getattr(self,method_name)
            output=method(input)
            result=str(output)
            
            # 连接输出Redis
            with Redis(self.redis_ip, self.redis_port, db=0, socket_timeout=5) as r_out:
                # 写入结果
                r_out.set(output_key, result)
            
    except ConnectionError as e:
        print(f"Redis connection error: {str(e)}")
    except Exception as e:
        print(f"Error in worker thread: {str(e)}")
