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

    def __init__(self):
        # 用户本地的日志由代理服务器维护
        self.log = []
        # 连接到服务器，转发客户端的请求
        self.conn = rpyc.connect("localhost", 8000)
        self.service = self.conn.root

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
        这些功能要转发给服务器进行处理
        :param user_id:
        :param key:
        :return: result
        """
        result = self.service.exposed_get_key(user_id, key)
        self.log_operation("get", key, result)
        return result

    def exposed_put_key(self, user_id, key, value):
        """
        转发给服务器去处理
        :param user_id:
        :param key:
        :param value:
        :return:
        """
        self.service.exposed_put_key(user_id, key, value)
        self.log_operation("put", key, value)
        return "success"

    def exposed_del_key(self, user_id, key):
        """
        转发给服务器去处理
        :param user_id:
        :param key:
        :return:
        """
        deleted_value = self.service.exposed_del_key(user_id, key)
        self.log_operation("delete", key, deleted_value)
        return deleted_value

    def exposed_get_log(self):
        return self.log

    def exposed_get_global_log(self):
        """
        全局的日志也转发到服务器去返回
        :return: global log
        """
        return self.service.exposed_get_global_log()


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
