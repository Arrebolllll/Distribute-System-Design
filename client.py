import sys

import rpyc


def format_log(log):
    print("{:<12} {:<12} {:<12}".format("Operation", "Key", "Value"))
    for entry in log:
        operation = entry["operation"]
        key = entry["key"]
        value = entry["value"] if entry["value"] is not None else ""
        print("{:<12} {:<12} {:<12}".format(operation, key, value))


class KeyValueClient:
    def __init__(self):
        self.conn = rpyc.connect("localhost", 8000)
        self.service = self.conn.root

    def run_client(self):
        while True:
            command = input("Enter command (get/put/del/log/exit): ").lower()

            if command == 'exit':
                break
            elif command.startswith('get'):
                try:
                    _, key = command.split()
                    result = self.service.exposed_get_key(key)
                    print(f"Result: {result}")
                except ValueError:
                    print("Invalid format. Usage: get <key>")
            elif command.startswith('put'):
                try:
                    _, key, value = command.split()
                    result = self.service.exposed_put_key(key, value)
                    print(f"Result: {result}")
                except ValueError:
                    print("Invalid format. Usage: put <key> <value>")
            elif command.startswith('del'):
                try:
                    _, key = command.split()
                    result = self.service.exposed_del_key(key)
                    print(f"Result: {result}")
                except ValueError:
                    print("Invalid format. Usage: del <key>")
            elif command.startswith('log'):
                result = self.service.exposed_get_log()
                format_log(result)
            else:
                print("Invalid command. Please enter get/put/del/log/exit.")


if __name__ == "__main__":
    client = KeyValueClient()
    client.run_client()
