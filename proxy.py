import rpyc
from rpyc.utils.server import ThreadedServer
import threading
import time


class Proxy(rpyc.Service):
    # 用户名和密码
    users = {
        'o': 'o',
        's': 's',
        'j': 'j',
    }
    # 最多允许五个客户登录
    client_conn = [False] * 5
    global_log = []

    def __init__(self):
        self.data_store = {}
        self.log = []

    def get_id(self):
        """
        分配用户id
        :return: 用户id
        """
        for i, conn in enumerate(self.client_conn):
            if not conn:
                self.client_conn[i] = True
                print("client {} verified".format(i))
                return i
        print("代理服务器已经到达最大连接数量，无法继续连接")
        return None

    def on_connect(self, conn):
        print("Client connected")

    def on_disconnect(self, conn):
        print("Client disconnected")

    def log_operation(self, user_id, operation, key, value=None):
        log_entry = {"operation": operation, "key": key, "value": value}
        global_log_entry = {"user": user_id, "operation": operation, "key": key, "value": value}
        self.log.append(log_entry)  # 每个用户自己的log
        self.global_log.append(global_log_entry)  # 看到全局的操作

    def exposed_login(self, username, password):
        if self.users[username] == password:
            user_id = self.get_id()
            return user_id
        return None

    def exposed_get_key(self, user_id, key):
        print(f"GET: {key}")
        result = self.data_store.get(key, None)
        self.log_operation(user_id, "get", key, result)
        return result

    def exposed_put_key(self, user_id, key, value):
        print(f"PUT: {key}={value}")
        self.data_store[key] = value
        self.log_operation(user_id, "put", key, value)
        return "success"

    def exposed_del_key(self, user_id, key):
        print(f"DELETE: {key}")
        deleted_value = self.data_store.pop(key, None)
        self.log_operation(user_id, "delete", key, deleted_value)
        return deleted_value

    def exposed_get_log(self):
        return self.log

    def exposed_get_global_log(self):
        return self.global_log


def run_server():
    server = ThreadedServer(Proxy, port=7888)
    print("Server listening on port 7888...")

    # 输出当前活动的线程数量
    def print_active_threads():
        while True:
            active_threads = threading.active_count()
            print(f"Active threads: {active_threads}")
            time.sleep(7)

    thread = threading.Thread(target=print_active_threads)
    thread.daemon = True
    thread.start()
    server.start()


if __name__ == "__main__":
    run_server()
