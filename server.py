import rpyc
from rpyc.utils.server import ThreadedServer
import threading
import time

# 集中式数据库和日志，需要互斥访问
data_store = {}
log = []
lock = threading.Lock()


class KeyValueService(rpyc.Service):
    def on_connect(self, conn):
        print("Connected to proxy")

    def on_disconnect(self, conn):
        pass

    def log_operation(self, user_id, operation, key, value=None):
        # with lock要删去:
        global_log_entry = {"user": user_id, "operation": operation, "key": key, "value": value}
        log.append(global_log_entry)  # 看到全局的操作

    def exposed_get_key(self, user_id, key):
        with lock:
            # 生成散列值，根据usr_id和key
            key = hash(str(user_id) + str(key))
            print(f"GET: {key}")
            result = data_store.get(key, None)
            self.log_operation(user_id, "get", key, result)
            return result

    def exposed_put_key(self, user_id, key, value):
        with lock:
            key = hash(str(user_id) + str(key))
            print(f"PUT: {key}={value}")
            data_store[key] = value
            self.log_operation(user_id, "put", key, value)
            return "success"

    def exposed_del_key(self, user_id, key):
        with lock:
            key = hash(str(user_id) + str(key))
            print(f"DELETE: {key}")
            deleted_value = data_store.pop(key, None)
            self.log_operation(user_id, "delete", key, deleted_value)
            return deleted_value

    def exposed_get_global_log(self):
        return log


def run_server():
    server = ThreadedServer(KeyValueService, port=8000)
    print("Server listening on port 8000...")
    server.start()


if __name__ == "__main__":
    run_server()
