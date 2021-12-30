"""CD Chat client program"""
import logging
import socket
import selectors
from .protocol import CDProto, CDProtoBadFormat
import sys
import fcntl
import os
import json
import time
logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)


class Client:
    """Chat Client process."""
    
    def __init__(self, name: str = "Foo"):
        """Initializes chat client."""
        #cliente a tem um nome de atributo que vai ser o user no protocolo
        #atributos
        self.name=name
        self.sel=selectors.DefaultSelector()
        self.protocolo=CDProto()
        self.channel=None
        self.socket=""


    def receberServidor(self,socket,mask):
        """Aqui vai ser lido o valor do servidor"""
        msg=self.protocolo.recv_msg(socket)
        logging.debug("received: %s",msg)
        if (hasattr(msg, 'channel')):
            print("{}-> {}".format(msg.__getattribute__("channel"),msg.__getattribute__("message")))
        else:
            print("{}".format(msg.__getattribute__("message")))


    def ler_input(self,input,mask):
        """Aqui vai ser lido o valor de input"""
        
        msg=input.read().strip() # retirar o \n com o strip
        #criar mensagem
        if (msg == 'exit'): #apagar 
            self.sel.unregister(self.socket)
            self.socket.close()
            self.sel.unregister(input)
            sys.exit(0)

        elif (msg[0:5] == '/join'):
            msg=msg[6:]
            juntar=self.protocolo.join(msg)
            self.protocolo.send_msg(self.socket,juntar.__dict__)
            self.channel=msg

        else : #mensagem normal
            if (self.channel == None):
                msg_Prot=self.protocolo.message(msg)
                print(msg_Prot)
                self.protocolo.send_msg(self.socket,msg_Prot.__dict__)
                
            else:
                msg_Prot=self.protocolo.message(msg,self.channel)
                self.protocolo.send_msg(self.socket,msg_Prot.__dict__)
                

    def connect(self):
        """Connect to chat server and setup stdin flags."""
        #aqui vou ter que ligar ao RegisterMessager indicando-lhe o name 
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect(('localhost', 1234))
        self.socket=sock
        #ir buscar o atributo do name 
        msg=self.protocolo.register(self.name) 
       
        self.protocolo.send_msg(sock,msg.__dict__)
        
        # comandos para fazer o cliente nao bloquear
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)
        #colocar o seletor a ler ou a receber 
        self.sel.register(sys.stdin, selectors.EVENT_READ,self.ler_input)
        self.sel.register(sock, selectors.EVENT_READ,self.receberServidor)

        

    def loop(self):
        """Loop indefinetely."""
        while True: 
            sys.stdout.write('')
            sys.stdout.flush()
            for key, mask in self.sel.select():
                callback = key.data
                callback(key.fileobj, mask)
            
