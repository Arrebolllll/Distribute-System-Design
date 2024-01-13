import sys

import rpyc


def format_log(log):
    print("{:<12} {:<12} {:<12}".format("Operation", "Key", "Value"))
    for entry in log:
        operation = entry["operation"]
        key = entry["key"]
        value = entry["value"] if entry["value"] is not None else ""
        print("{:<12} {:<12} {:<12}".format(operation, key, value))


def format_global_log(log):
    print("{:<12} {:<12} {:<12} {:<12}".format("user", "Operation", "Key", "Value"))
    for entry in log:
        user = entry["user"]
        operation = entry["operation"]
        key = entry["key"]
        value = entry["value"] if entry["value"] is not None else ""
        print("{:<12} {:<12} {:<12} {:<12}".format(user, operation, key, value))


class KeyValueClient:
    def __init__(self):
        # 转为连接到代理服务器，代理服务器端口为7888
        self.conn = rpyc.connect("localhost", 7888)
        self.service = self.conn.root

    def login(self):
        """
        Login to the server
        :return: user_id
        """
        username = input("Enter your username: ")
        password = input("Enter your password: ")
        user_id = self.service.exposed_login(username, password)
        return user_id

    def run_client(self):
        user_id = self.login()
        while user_id is not None:
            command = input("Enter command (get/put/del/log/exit): ").lower()

            if command == 'exit':
                break
            elif command.startswith('get'):
                try:
                    _, key = command.split()
                    result = self.service.exposed_get_key(user_id, key)
                    print(f"Result: {result}")
                except ValueError:
                    print("Invalid format. Usage: get <key>")
            elif command.startswith('put'):
                try:
                    _, key, value = command.split()
                    result = self.service.exposed_put_key(user_id, key, value)
                    print(f"Result: {result}")
                except ValueError:
                    print("Invalid format. Usage: put <key> <value>")
            elif command.startswith('del'):
                try:
                    _, key = command.split()
                    result = self.service.exposed_del_key(user_id, key)
                    print(f"Result: {result}")
                except ValueError:
                    print("Invalid format. Usage: del <key>")
            elif command.startswith('log'):
                try:
                    if len(command.split()) == 1:
                        result = self.service.exposed_get_log()
                        format_log(result)
                    else:
                        _, arg1 = command.split()
                        if arg1 == '-g':
                            result = self.service.exposed_get_global_log()
                            format_global_log(result)
                except ValueError:
                    print("Invalid argument. Usage: log -g")
            else:
                print("Invalid command. Please enter get/put/del/log/exit.")
        else:
            print("Invalid username/password")


if __name__ == "__main__":
    client = KeyValueClient()
    client.run_client()
