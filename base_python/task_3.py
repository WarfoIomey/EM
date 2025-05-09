class Server:
    """Работа серверов в сети."""
    _counter = 0

    def __init__(self, router=None):
        Server._counter += 1
        self.ip_address = Server._counter
        self.router = router
        self.packegs = []

    def send(self, data, server):
        if self.router is not None:
            router.buffer.append((data, server))

    def __str__(self):
        return f'IP-адрес сервера: {self.ip_address}'


class Router:
    """Работа роутера в сети."""

    def __init__(self, servers=[]):
        self.servers = servers
        self.buffer = []

    def link(self, server):
        self.servers.append(server)
        server.router = self

    def unlink(self, server):
        self.servers.remove(server)
        server.router = None

    def send_data(self):
        for g in self.buffer:
            self.servers[self.servers.index(g[1])].packegs.append(g[0])
        self.buffer = []


class Data:
    """Пакеты информации."""

    def __init__(self, data):
        self.data = data


if __name__ == '__main__':
    sv = Server()
    sv2 = Server()
    router = Router()
    router.link(sv)
    router.link(sv2)
    data = Data('Hello my friend')
    data_two = Data('dont hello')
    sv.send(data, sv2)
    sv.send(data_two, sv2)
    print(router.buffer)
    router.send_data()
    print(router.buffer)
    print(sv2.packegs)
