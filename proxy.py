import random
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
    # 与所有服务器取得连接
    servers = [
        ("localhost", 8000),
        ("localhost", 8001),
        ("localhost", 8002),
    ]
    conn_list = [rpyc.connect(server[0], server[1]).root for server in servers]

    def __init__(self):
        # 用户本地的日志由代理服务器维护
        self.log = []

    def on_connect(self, conn):
        print("Client connected")

    def on_disconnect(self, conn):
        print("Client disconnected")

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

    def log_operation(self, operation, key, value=None):
        log_entry = {"operation": operation, "key": key, "value": value}
        self.log.append(log_entry)  # 每个用户自己的log

    def exposed_login(self, username, password):
        """
        由代理服务器来实现登录功能
        :param username:
        :param password:
        :return:
        """
        if self.users[username] == password:
            user_id = self.get_id()
            return user_id
        return None

    def exposed_get_key(self, user_id, key):
        """
        这些功能要转发给服务器进行处理，随机选择一个服务器
        :param user_id:
        :param key:
        :return: result
        """
        index = random.randrange(len(self.conn_list))
        print(f"Forwarding GET request to server at {8000 + index}")
        result = self.conn_list[index].exposed_get_key(user_id, key)

        self.log_operation("get", key, result)
        return result

    def exposed_put_key(self, user_id, key, value):
        """
        转发给服务器去处理，随机选择一个服务器
        :param user_id:
        :param key:
        :param value:
        :return:
        """
        index = random.randrange(len(self.conn_list))
        print(f"Forwarding PUT request to server at {8000 + index}")
        self.conn_list[index].exposed_put_key(user_id, key, value)

        self.log_operation("put", key, value)
        return "success"

    def exposed_del_key(self, user_id, key):
        """
        转发给服务器去处理，随机选择一个服务器
        :param user_id:
        :param key:
        :return:
        """
        index = random.randrange(len(self.conn_list))
        print(f"Forwarding DELETE request to server at {8000 + index}")
        deleted_value = self.conn_list[index].exposed_del_key(user_id, key)

        self.log_operation("delete", key, deleted_value)
        return deleted_value

    def exposed_get_log(self):
        return self.log

    def exposed_get_global_log(self):
        """
        全局的日志也转发到服务器去返回
        :return: global log
        """
        index = random.randrange(len(self.conn_list))
        print(f"Forwarding log -g request to server at {8000 + index}")
        result = self.conn_list[index].exposed_get_global_log()
        return result


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
