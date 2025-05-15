class Server:
    """Работа серверов в сети."""
    _counter = 0

    def __init__(self, router=None, buffer=[]):
        Server._counter += 1
        self.ip_address = Server._counter
        self.router = router
        self.buffer = buffer
        self.packegs = []

    def send(self, data):
        if self.router is not None:
            self.router.buffer.append((data.data, data.server))

    def get_ip(self):
        return self.ip_address

    def get_data(self):
        data = self.packegs
        self.packegs = []
        return data

    def __str__(self):
        return f'IP-адрес сервера: {self.ip_address}'


class Router:
    """Работа роутера в сети."""

    def __init__(self, servers=[], buffer=[]):
        self.servers = servers
        self.buffer = buffer

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

    def __init__(self, data, server):
        self.data = data
        self.server = server
