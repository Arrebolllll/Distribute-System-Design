import rpyc
from rpyc.utils.server import ThreadedServer
import threading

# 共享的数据和锁
data_store = {}
log = []
data_store_lock = threading.Lock()


class KeyValueService(rpyc.Service):
    def __init__(self):
        # 服务器的缓存
        self.cache = {}

    def on_connect(self, conn):
        print("Connected to proxy")

    def on_disconnect(self, conn):
        pass

    def log_operation(self, user_id, operation, key, value=None):
        global_log_entry = {"user": user_id, "operation": operation, "key": key, "value": value}
        log.append(global_log_entry)

    def exposed_get_key(self, user_id, key):
        key = hash(str(user_id) + str(key))
        # 检查缓存
        if key in self.cache:
            # 缓存有就返
            return self.cache[key]
        with data_store_lock:
            # 没有就查数据库
            print(f"GET: {key}")
            result = data_store.get(key, None)
            self.log_operation(user_id, "get", key, result)
            # 然后加到缓存之中
            self.cache[key] = result
            return result

    def exposed_put_key(self, user_id, key, value):
        if key in self.cache:
            self.cache[key] = value
        with data_store_lock:
            key = hash(str(user_id) + str(key))
            print(f"PUT: {key}={value}")
            data_store[key] = value
            self.log_operation(user_id, "put", key, value)
            return "success"

    def exposed_del_key(self, user_id, key):
        with data_store_lock:
            key = hash(str(user_id) + str(key))
            print(f"DELETE: {key}")
            deleted_value = data_store.pop(key, None)
            self.log_operation(user_id, "delete", key, deleted_value)
            return deleted_value

    def exposed_get_global_log(self):
        print(log)
        return log


def run_server(port):
    server = ThreadedServer(KeyValueService, port=port)
    print(f"Server listening on port {port}...")
    server.start()


if __name__ == "__main__":
    num_servers = 3
    threads = []

    for i in range(num_servers):
        port = 8000 + i
        thread = threading.Thread(target=run_server, args=(port,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
