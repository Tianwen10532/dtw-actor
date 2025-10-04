from dtwactor.decorator import dtwactor


############################# user-define
@dtwactor
class AddWorker:
    def __init__(self):
        self.accu=0;
    
    def add(self, input:str) -> str:
        input = int(input)
        # time.sleep(10)
        self.accu+=input
        # return str(input+1)
        return str(self.accu)
    
    def double(self, input:str) -> str:
        input=int(input)
        # time.sleep(10)
        return str(input*2)

wk_cls = AddWorker
############################# user-define


import os

redis_ip = os.environ.get("REDIS_IP","192.168.117.144")
redis_port = os.environ.get("REDIS_PORT", "32000")
serve_port = os.environ.get("SERVE_PORT","8080")

wk = wk_cls()
redis_conf={"host":redis_ip,"port":redis_port}
wk.bind_redis(redis_conf)
wk.serve(8080)

