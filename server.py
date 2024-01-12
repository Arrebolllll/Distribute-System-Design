from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

class KeyValueServer:
    def __init__(self):
        self.data_store = {}
        self.log = []

    def get_key(self, key):
        value = self.data_store.get(key, None)
        self.log.append(f"GET: {key}")
        return value

    def put_key(self, key, value):
        self.data_store[key] = value
        self.log.append(f"PUT: {key}={value}")
        return "success"

    def del_key(self, key):
        value = self.data_store.pop(key, None)
        self.log.append(f"DELETE: {key}")
        return value

    def get_log(self):
        return self.log

def run_server():
    server = SimpleXMLRPCServer(('localhost', 8000), logRequests=True, allow_none=True)
    server.register_instance(KeyValueServer())
    print("Server listening on port 8000...")
    server.serve_forever()

if __name__ == "__main__":
    run_server()
