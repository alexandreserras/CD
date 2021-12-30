# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
import time
# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None

cache = {}
order = []

# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.server_to_use = 0

    def select_server(self):
        server = self.servers[self.server_to_use]

        self.server_to_use = self.server_to_use + 1
        if self.server_to_use == len(self.servers):
            self.server_to_use = 0

        return server
    
    def update(self, *arg):
        pass


# least connections policy
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        self.server_connections = {}

        for server in self.servers:
            self.server_connections[server] = []

    def select_server(self):
        least_connections = len(self.server_connections[self.servers[0]])
        ret_server = self.servers[0]

        i = 0
        for server in self.server_connections.keys():
            i += 1
            if len(self.server_connections[server]) < least_connections:
                least_connections = len(self.server_connections[server])
                ret_server = server

        return ret_server

    def update(self, *arg):
        arguments = arg[0]
        if arguments[0] == 'add':
            self.server_connections[arguments[1]].append(arguments[2])

        elif arguments[0] == 'del':
            self.server_connections[arguments[1]].remove(arguments[2])


# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        self.server_connections = {}
        self._initialTime = {}
        self._finalTime = 0
        self._const = 0.0003 # valor para impedir que o primeiro servidor seja escolhido imensas vezes
        for server in self.servers:
            self._initialTime[server] = []
            self.server_connections[server] = [0, 0]


    def select_server(self):
        mini = 99999
        return_server = None

        for server in self.server_connections.keys():
            if (self.server_connections[server][0] < mini):
                return_server = server
                mini = self.server_connections[server][0]

        if (self.server_connections[return_server][0] == 0):
            self.server_connections[return_server][0] = self._const

        print("wediwed")

        return return_server

    def update(self, *arg):
        arguments = arg[0]
        if arguments[0] == 'add':
            self._initialTime[arguments[1]].append((int)(time.time()))

        elif arguments[0] == 'del':
            self._finalTime = (int)(time.time())
            time_res = self._finalTime - self._initialTime[arguments[1]][0]
            self._initialTime[arguments[1]].pop(0)
            if (self.server_connections[arguments[1]][1] == 0):
                self.server_connections[arguments[1]] = [time_res, 1]
            else:
                # avg = (media * n_medicoes + time_res) / n_medicoes + 1
                avg = self.server_connections[arguments[1]][0] * self.server_connections[arguments[1]][1]
                avg = avg + time_res
                avg = avg / (self.server_connections[arguments[1]][1] + 1)
                self.server_connections[arguments[1]] = [avg, self.server_connections[arguments[1]][1] + 1]

POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}
        self.map_servers = {}

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] =  upstream_sock
        self.map_servers[client_sock] =  upstream_server
        self.policy.update(('add', upstream_server, client_sock))
        # chamar o policy.update e passar o upstream_server, o comando 'add' e a client_sock

    def delete(self, sock):
        try:        
            server = self.get_upstream_server(sock)
            sel.unregister(sock)
            sock.close()
            self.policy.update(('del', server, sock))
            if sock in self.map:
                self.map.pop(sock)
        except KeyError:
            pass

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_upstream_server(self, sock):
        return self.map_servers.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    mapper.add(client, policy.select_server())

def read(conn,mask):
    send = True

    data = conn.recv(4096)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        req = data.decode("utf-8").split(" ")
        
        if req[0] == "GET":
            precision = req[1].replace("/", "")
            print(precision)
            print(precision in cache.keys())

            if (precision in cache.keys()):
                conn.send(cache[precision])
                send = False
                print("-------VOU MANDAR EU-------")
            
        elif req[0] == "<!DOCTYPE":
            answer = data.decode("utf-8").split("\n")
            precision = answer[9].strip().replace("<h1> Computing Pi with precision ", "").replace(" </h1>", "")
            print("A responder a: {}".format(precision))
            print(cache.keys())

            if precision not in cache.keys():
                # tenho de ver se já existem 5
                # # se não adiciona
                if (len(order) < 5):
                    order.append(precision)
                    cache[precision] = data
                else:
                    prec_2_del = order[0]
                    for i in range(1, 5): # [1, 2, 3, 4]
                        order[i-1] = order[i]

                    order[4] = precision
                    cache.pop(prec_2_del, None)
                    cache[precision] = data
                # se sim tenho de trocar as posições do ordem (o 0 é eliminado, o 1 passa a 0, o 2 a 1, etc etc)
            else:
                # pode atualizar a resposta
                cache[precision] = data

            """
            for prec in cache.keys():
                if prec in answer[9]:
                    print("{} está na {}".format(prec, answer[9]))
                    cache[prec] = data
            """

        if send:
            mapper.get_sock(conn).send(data)


def main(addr, servers, policy_class):
    global policy
    global mapper

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])
