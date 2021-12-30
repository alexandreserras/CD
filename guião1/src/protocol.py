"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket
import time

class Message:
    """Message Type."""
    #command atributo
    def __init__(self, command):
        """Initializes chat client."""
        self.command=command
    def __repr__(self):
         return f'{{"command": "{self.command}"}}'
        
        
class JoinMessage(Message):
    """Message to join a chat channel."""
    #channel atributo
    def __init__(self, command,channel):
        super().__init__(command)
        self.channel=channel
    
    def __repr__(self):
        return f'{{"command": "join", "channel": "{self.channel}"}}'

class RegisterMessage(Message):
    """Message to register username in the server."""
    #user atributo
    def __init__(self, command,user):
        super().__init__(command)
        self.user=user 
    
    def __repr__(self):
        return f'{{"command": "register", "user": "{self.user}"}}'


    
       
    
class TextMessage(Message):
    """Message to chat with other clients."""
    #message atributo
    #ts atributo
    def __init__(self,command,message,channel=None):
        if channel == None:     
            super().__init__(command)
            self.message=message
            self.ts=int(time.time())
            self.channel=None
        else:
            super().__init__(command)
            self.message=message
            self.ts=int(time.time())
            self.channel=channel

    def __repr__(self):  
        if self.channel == None:
            return f'{{"command": "message", "message": "{self.message}", "ts": {self.ts}}}' 
        else:
            return f'{{"command": "message", "message": "{self.message}", "channel": "{self.channel}", "ts": {self.ts}}}'


class CDProto:
    """Computação Distribuida Protocol."""


    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        registo= RegisterMessage("register",username)
        return registo
        


    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage("join",channel)

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        if channel == None:
            return TextMessage("message",message)
        else:
            return TextMessage("message",message,channel)


    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        msg_json=(json.dumps(msg).encode('utf8'))
        total_bytes=len(msg_json)
        if total_bytes>= 2**16:
            raise CDProtoBadFormat
        connection.send(len(msg_json).to_bytes(2,byteorder='big'))
        connection.send(msg_json) # forma para converter e enviar dados 


    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        #Problema do control + c
        tamanho=connection.recv(2) #aqui só vou ler 2 bytes
        total_bytes=int.from_bytes(tamanho,byteorder='big') #assim ja sei quantos bytes tenho de ler 
        if total_bytes>= 2**16:
            raise CDProtoBadFormat
        if  (total_bytes) > 0:
            ok=0
            while(ok == 0):
                try:
                    data = connection.recv(total_bytes)  #fico em data com tudo o que precisar restando apenas voltar a tornar string
                    ok=1
                except:
                    ok=0
        
        try:
            d_descodificada=json.loads(data) #assim ja se encontra num dicionario de novoW
            if (d_descodificada["command"]=="message"):
                if ( 'channel' in d_descodificada):
                    return TextMessage("message",d_descodificada["message"],d_descodificada["channel"])
                else:
                    return TextMessage("message",d_descodificada["message"])

            elif(d_descodificada["command"]=="register"):
                    return RegisterMessage("register",d_descodificada["user"])
            else :
                return JoinMessage("join",d_descodificada["channel"])
            
        except ValueError :
           
            raise CDProtoBadFormat
        
       


class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
