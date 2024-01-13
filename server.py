import rpyc
from rpyc.utils.server import ThreadedServer


class KeyValueService(rpyc.Service):
    def __init__(self):
        self.data_store = {}
        self.log = []

    def on_connect(self, conn):
        print("Client connected")

    def on_disconnect(self, conn):
        print("Client disconnected")

    def log_operation(self, operation, key, value=None):
        log_entry = {"operation": operation, "key": key, "value": value}
        self.log.append(log_entry)
        print(f"Logged operation: {log_entry}")

    def exposed_get_key(self, key):
        print(f"GET: {key}")
        result = self.data_store.get(key, None)
        self.log_operation("get", key, result)
        return result

    def exposed_put_key(self, key, value):
        print(f"PUT: {key}={value}")
        self.data_store[key] = value
        self.log_operation("put", key, value)
        return "success"

    def exposed_del_key(self, key):
        print(f"DELETE: {key}")
        deleted_value = self.data_store.pop(key, None)
        self.log_operation("delete", key, deleted_value)
        return deleted_value

    def exposed_get_log(self):
        return self.log


def run_server():
    server = ThreadedServer(KeyValueService, port=8000)
    server.data_store = {}
    server.log = []
    print("Server listening on port 8000...")
    server.start()


if __name__ == "__main__":
    run_server()
