import time
import socket, base64
import sys
import argparse
from string import ascii_letters,digits,ascii_lowercase,ascii_uppercase
import random
from itertools import product
import struct
import sys
import json

from const import (
    BANNED_TIME,
    COOLDOWN_TIME,
    NEW_PENALTY,
    MIN_VALIDATE,
    MAX_VALIDATE,
    MIN_TRIES,
    MAX_TRIES,
    PASSWORD_SIZE,
)

class Slave:
    def __init__(self) :
        self.wrong_passwords = []
        self.slaves =[]
        self.port =8000
        self.connected = False
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s.settimeout(1)
        
        self.ipslave=socket.gethostbyname(socket.gethostname())
        print(self.ipslave)
        self.receive_message_multicast()
        # self.send_multi_cast_message({"command":"connecting","ip":self.ipslave})
        self.all_pass=self.gerar(PASSWORD_SIZE)
        
    def send_multi_cast_message(self,msg)->None:
        multicast_group = ('224.3.29.71', 10000)
        ttl = 1
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM,socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        sock.settimeout(0.2)
        try:
            # Send data to the multicast group
            msg_envio = msg
            sock.sendto(json.dumps(msg_envio).encode("utf-8"), (multicast_group))
            print(json.dumps(msg_envio))
        finally:
            sock.close()
    
    def receive_message_multicast(self):
        multicast_group = '224.3.29.71'
        server_address = ('', 10000)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM,socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1)
        sock.bind(server_address)
        mreq = struct.pack("4sl", socket.inet_aton(multicast_group), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        print("RECEIVE")
        tamanho = len(self.slaves)
        msg =""
        while True:
            try:
                #esta mal
                print("entrei")
                msg=json.loads(sock.recv(10240).decode("utf-8"))
            except socket.timeout:
                # provavelmente a socket ainda está sozinha

                break
            if (msg != ""):
                if (msg["command"]=="connecting"):
                    args=[tamanho]
                    self.command_execute_multicast(msg,args)
                if (msg["command"]=="keep_alive"):
                    self.command_execute_multicast(msg,[])

            msg=""
            tamanho =len(self.slaves)

    
    def command_execute_multicast(self,msg,args):
        if (msg["command"]=="connecting"):
                    ret=msg["ip"]
                    print(ret)
                    if ( ret not in self.slaves):
                        print("add")
                        self.slaves.append(ret)
                    print("ok"+ret)
                    if (len(self.slaves)>args[0]):
                        print("vou responder")
                        
                        msg_envio = {"command":"connecting_ack", "slave":self.slaves,"ip":self.ipslave,"wrongs":self.wrong_passwords}
                        self.send_message(ret,msg_envio)
                        print(msg_envio)
        elif (msg["command"]=="connecting_ack"):
            self.connected = True
            lista=msg["slave"]
            print(lista)
            for i in lista:
                if (i not in self.slaves and i != self.ipslave):
                    self.slaves.append(i)
            if (msg["ip"] not in self.slaves):
                self.slaves.append(msg["ip"])
            for j in msg["wrongs"]:

                if (j not in self.wrong_passwords):
                    self.wrong_passwords.append(j)
        elif (msg["command"]=="keep_alive"):
            if not self.connected:
                self.send_message(msg["ip"], {"command":"connecting","ip":self.ipslave})
            lista=msg["slave"]
            print(lista)
            for i in lista:
                if (i not in self.slaves and i != self.ipslave):
                    self.slaves.append(i)
            if (msg["ip"] not in self.slaves):
                self.slaves.append(msg["ip"])
            for j in msg["wrongs"]:
                if (j not in self.wrong_passwords):
                    self.wrong_passwords.append(j)
                
    
    def send(self, address, msg):
        """ Send msg to address. """
        self.s.sendto(msg, address)
    def recv(self):
        """ Retrieve msg payload and from address."""
        while True:
            try:
                payload, addr = self.s.recvfrom(1024)
            except socket.timeout:
                return None, None

            if len(payload) == 0:
                return None, addr
            return payload, addr

    def send_message(self,ip,message):
        message=json.dumps(message).encode("utf-8")
        self.send((ip,8000),message)
        print("enviei")

    def receive_message(self):
        #preciso de ligar a socket, ver qual  é o tipo de messagem que estou a receber e consoante o que for
        #tratar do problema
    
        msg=""
        try:
            payload, addr = self.s.recvfrom(1024)
        except socket.timeout:
            return

        if len(payload) != 0:
            msg =json.loads(payload).decode("utf-8")
            print("received message: %s" % data)
                
        if (msg != ""):
            if (msg["command"]=="connecting_ack"):
                args=[]
                self.command_execute_multicast(msg,args)                 
                
    def gerar(self,length)-> list:
        ret=[]
        for i in product(ascii_letters + digits, repeat=length):
            ret.append(''.join(i))
        return ret

    def ret_pass(self) ->list:
        return self.all_pass

    def find_password(self,ip='172.17.0.2'):
        host= ip
        port= 8000
        username= 'root'
        combinacoes=self.ret_pass()
        msg_array = []
        n_tries = 0
        index =0
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        while index<len(combinacoes):

            password= combinacoes[index]
            #print(password)
            index+=1
            token= base64.encodebytes(('%s:%s' % (username, password)).encode()).strip()
            lines= 'GET / HTTP/1.1\nHost: %s\nAuthorization: Basic %s\nConnection: keep-alive\n\n' % ( host, token.decode())
            
            s.send(lines.encode('utf-8'))

            recv = s.recv(1024).decode("utf-8")

            #print(recv)

            msg_array = recv.split("\r\n")

           # print(msg_array)

            if (msg_array[0] == 'HTTP/1.1 200 OK'):
                # avisar que já descobriu a pass
                print("Acertou, parabéns")
                break
            n_tries += 1
            self.wrong_passwords.append(password)
            
            if n_tries % MIN_TRIES ==  0:
                print("---------------------entrei---------------------------")
                #print(n_tries)
                print(self.slaves)
                self.receive_message_multicast()
                self.receive_message()
                self.send_multi_cast_message({"command":"keep_alive","slave":self.slaves,"ip":self.ipslave,"wrongs":self.wrong_passwords})
                time.sleep(COOLDOWN_TIME/1000)
                print(self.wrong_passwords)
                print("A MINHA LISTA ATUAL")
                print(self.slaves)
        s.close()


# sys.argv -> para ir buscar argumentos
if __name__ == "__main__":
    

    print(sys.argv)

    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", help="IP do servidor que está no docker")
    args = parser.parse_args()

    ip = '172.17.0.2'

    if (len(sys.argv) > 1 and sys.argv[1] == '-ip'):
        print(sys.argv[2])
        ip = sys.argv[2]
    
    slave =Slave()
    slave.find_password()
        

# Mensagens do protocolo
# Mensagem de registo: msg = {"command": "register", "ip": "ip_do_slave"}

