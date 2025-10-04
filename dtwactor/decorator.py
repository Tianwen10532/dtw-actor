
from dtwactor.dtwactor_method import bind_redis,serve,handle_client,redis_worker

methods = {
    'bind_redis': bind_redis,
    'serve': serve,
    'handle_client': handle_client,
    'redis_worker': redis_worker,
}

def dtwactor(cls):
    
    for name, func in methods.items():
        setattr(cls, name, func)
    
    # # @classmethod
    # def remote(self, host="0.0.0.0", port=8000):
    #     self.serve(port)
        
    # cls.remote = remote
    
    return cls
