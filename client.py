import xmlrpc.client

class KeyValueClient:
    def __init__(self):
        self.server = xmlrpc.client.ServerProxy("http://localhost:8000")

    def run_client(self):
        while True:
            full_command = input("Enter command (put/del/get/log/exit): ").lower()

            if full_command == 'exit':
                break
            elif full_command.startswith('put'):
                try:
                    _, key, value = full_command.split()
                    result = self.server.put_key(key, value)
                    print(f"Result: {result}")
                except ValueError:
                    print("Invalid format. Usage: put <key> <value>")
            elif full_command.startswith('get'):
                try:
                    _, key = full_command.split()
                    result = self.server.get_key(key)
                    print(f"Result: {result}")
                except ValueError:
                    print("Invalid format. Usage: get <key>")
            elif full_command.startswith('del'):
                try:
                    _, key = full_command.split()
                    result = self.server.del_key(key)
                    print(f"Result: {result}")
                except ValueError:
                    print("Invalid format. Usage: del <key>")
            elif full_command == 'log':
                result = self.server.get_log()
                print(f"Log entries: {result}")
            else:
                print("Invalid command. Please enter put/del/get/log/exit.")

if __name__ == "__main__":
    client = KeyValueClient()
    client.run_client()
