"""CD Chat server program."""
import logging
import selectors
import socket
from .protocol import CDProto, CDProtoBadFormat
import sys
import datetime
import json
logging.basicConfig(filename="server.log", level=logging.DEBUG)

class Server:
    """Chat Server process."""
    def __init__(self):
        """Initializes chat client."""
        #cliente a tem um nome de atributo que vai ser o user no protocolo
        self.sel=selectors.DefaultSelector()
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('localhost', 1234))
        
        sock.listen(100) # que valor colocar no listen?  
        self.sel.register(sock, selectors.EVENT_READ, self.accept) 
        self.clientes=[] #este dicionario vai ter que ficar IP : cliente resta saber como
        self.HEADER=2
        self.protocolo=CDProto()

    def accept(self,sock, mask):
        conn, addr = sock.accept()  #conn é a nova socket
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)


    def read(self,conn, mask):
        try:
            d_descodificada=self.protocolo.recv_msg(conn)
            print(d_descodificada)
            
            logging.debug("received: %s",d_descodificada)
            if (d_descodificada.__getattribute__("command") == 'register'):
                dic={'conn':conn,
                        'user':d_descodificada.__getattribute__("user"),
                        'channel':None  }
                self.clientes.append(dic)
                user=d_descodificada.__getattribute__("user")
                print(f"{user} registado com sucesso")
                #FALTA Informar que o user foi registado

            elif (d_descodificada.__getattribute__("command")== 'join'):
                for dados in self.clientes:
                    if dados.get('conn') == conn:
                        dados['channel']=d_descodificada.__getattribute__("channel")
                        print(f"{dados['user']} trocou de canal com sucesso")


            elif (d_descodificada.__getattribute__("command")== 'message'):     
                for dados in self.clientes:
                    if (hasattr(d_descodificada, 'channel')):
                        if dados.get('channel') == d_descodificada.__getattribute__("channel"):
                            msg_Prot=self.protocolo.message(d_descodificada.__getattribute__("message"),d_descodificada.__getattribute__("channel"))
                           
                            self.protocolo.send_msg(dados.get('conn'),msg_Prot.__dict__)
                                
                    else:
                        if dados.get('channel') == None:
                            msg_Prot=self.protocolo.message(d_descodificada.__getattribute__("message"))
                           
                            self.protocolo.send_msg(dados.get('conn'),msg_Prot.__dict__)
        except:
            for dados in self.clientes:
                if dados.get('conn') == conn:
                    self.clientes.remove(dados)
                    print(f"{dados.get('user')} apagado  com sucesso")
                    self.sel.unregister(conn)
                    conn.close()



            
            
            

    def loop(self):
        while True:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)









    
