class Server:
    """Работа серверов в сети."""
    _counter = 0

    def __init__(self, router=None, buffer=None):
        Server._counter += 1
        self.ip_address = Server._counter
        self.router = router
        self.buffer = buffer if buffer is not None else []
        self.packegs = []

    def send(self, data):
        if (
            self.router is not None
            and data.server is not None
            and data.server.router is not None
        ):
            self.router.buffer.append((data.data, data.server))

    def get_ip(self):
        return self.ip_address

    def get_data(self):
        data = self.packegs.copy()
        self.packegs = []
        return data

    def __str__(self):
        return f'IP-адрес сервера: {self.ip_address}'

    def __repr__(self):
        return f'Сервер {self.ip_address}'


class Router:
    """Работа роутера в сети."""

    def __init__(self, servers=None, buffer=None):
        self.servers = servers if servers is not None else []
        self.buffer = buffer if buffer is not None else []

    def link(self, server):
        self.servers.append(server)
        server.router = self

    def unlink(self, server):
        if server in self.servers:
            self.servers.remove(server)
            server.router = None

    def send_data(self):
        for data in self.buffer:
            if data[1] in self.servers:
                data[1].packegs.append(data[0])
        self.buffer = []


class Data:
    """Пакеты информации."""

    def __init__(self, data, server):
        self.data = data
        self.server = server
