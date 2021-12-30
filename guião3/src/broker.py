"""Message Broker"""
from typing import Dict, List, Any, Tuple
import selectors
import socket
import json
import pickle
import xml.etree.ElementTree as ET
import enum

# o brocker fala as 3 cenas ao mesmo tempo (traduz de uma para outra)

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2

    @classmethod
    def tostring(cls, val):
        if val == 0:
            return "JSON"
        elif val == 1:
            return "XML"
        elif val == 2:
            return "PICKLE"
        else:
            return "Not known"
    
    @classmethod
    def backSeri(cls, val):
        if val == 0:
            return Serializer.JSON
        elif val == 1:
            return Serializer.XML
        elif val == 2:
            return Serializer.PICKLE
    

class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        # print("[BROCKER] Starting...")
        self._sel = selectors.DefaultSelector()
        self._sock = socket.socket()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self._host = "localhost"
        self._port = 5000
        #LOGGER.info("Listen @ %s:%s", self._host, self._port)

        self._sock.bind((self._host, self._port))

        self._sock.listen()
        self._sel.register(self._sock, selectors.EVENT_READ, self.connect)

        self._clients = {}
        self._tree = {}
        self._topics=[]
        self._store_values = {}


        """ arvore: {"/": [], "/weather": [subscritos da MaryChannel], "/weather/temp": [subs]}
        se a mensagem for para weather/temp tem de mandar para os 
        subs de weather e de weather/temp """

    def recv_msg(self, connection: socket):
        """Receives through a connection a Message object."""
        
        msg_header = connection.recv(2)
       
        if not len(msg_header):
            exit(0)
            return False

        msg_len = int.from_bytes(msg_header, byteorder='big')
        msg_dec = connection.recv(msg_len)
        """ aqui vê se a socket existe e lê conforme a serialização usada
        OU se não existir é porque se está a juntar e, neste caso, usa json e adiciona ao dic"""
        if connection in self._clients.keys():
            # a socket já estava ligada e temos de ler como o consumer quer
            if self._clients.get(connection) == Serializer.JSON:
                msg_dec = msg_dec.decode("utf-8")
                ret = json.loads(msg_dec)
                return ret
            elif self._clients.get(connection) == Serializer.PICKLE:
                ret = pickle.loads(msg_dec)
                return ret
            elif self._clients.get(connection) == Serializer.XML:
                ret = ET.fromstring(msg_dec)
                dic_ret = {}
                for child in ret:
                    dic_ret[child.tag] = child.attrib["value"]
                return dic_ret
        else:
            # a socket quer-se ligar agora
            # lemos em json e adicionamos a socket ao dict com a serialização como method            
            msg_dic = json.loads(msg_dec)
            return msg_dic
                         
    def send_msg(self, dic):
        conns = self.list_subscriptions(dic["topic"])
        for conn in conns:
            if self._clients[conn[0]] == Serializer.JSON: 
                msg_enc = self.parse_msg_json(dic)
                msg_len = len(msg_enc).to_bytes(2, byteorder='big')
                conn[0].send(msg_len)
                conn[0].send(msg_enc)
            elif self._clients[conn[0]] == Serializer.PICKLE:
                msg_enc = self.parse_msg_pickle(dic)  
                msg_len = len(msg_enc).to_bytes(2, byteorder='big')
                conn[0].send(msg_len)
                conn[0].send(msg_enc)
            elif self._clients[conn[0]] == Serializer.XML:
                msg_enc = self.parse_msg_xml(dic)
                msg_len = len(msg_enc).to_bytes(2, byteorder='big')
                conn[0].send(msg_len)
                conn[0].send(msg_enc)

    def parse_msg_json(self, msg):
        msg_enc = json.dumps(msg).encode("utf-8")      
        return msg_enc

    def parse_msg_pickle(self, msg):
        msg_enc = pickle.dumps(msg)
        return msg_enc

    def parse_msg_xml(self, dic):
        print(dic)
        root = ET.Element('data')
        for key in dic.keys():
            ET.SubElement(root, str(key)).set("value", str(dic[key]))
        print("sdfsdfsf")
        return ET.tostring(root)

    def connect(self, sock: socket):
        conn, addr = self._sock.accept()
        print("[BROCKER] client ({}:{}) joined the server".format(addr[0], addr[1]))        
        self._sel.register(conn, selectors.EVENT_READ, self.read_msg)

    def read_msg(self, sock: socket):
        # lê a mensagem e chama a função respetiva
        try:
            ret = self.recv_msg(sock)
            print(ret)
            if ret and "command" in ret.keys():
                # REGISTER
                if ret["command"] == "register":
                    
                    self._clients[sock] = Serializer.backSeri(ret["method"])
                    print("[BROCKER] client of type {} will use {}".format(ret["type"], Serializer.tostring(ret["method"])))
                
                # SUBSCRIBE
                elif ret["command"] == "subscribe":                    
                    self.subscribe(ret["topic"], sock, self._clients[sock])
                
                # UNSUBSCRIBE
                elif ret["command"] == "unsubscribe":
                    # elimina a entrada do dicionário
                    # fecha a conexão
                    self.unsubscribe(ret["topic"], sock)
                    self._sel.unregister(sock)
                    sock.close()
                    for k in self._tree.keys():
                        if sock in self._tree[k]:
                            self._tree[k].remove(sock)
                    del self._clients[sock]
                
                # MESSAGE
                elif ret["command"] == "message":
                    self.send_msg(ret)
                    self.put_topic(ret["topic"], ret["msg"])
                    self._store_values[ret["topic"]]
                    
                # LIST ALL
                elif ret["command"] == "list_all":
                   lista= self.list_topics
                   #ver de que tipo é o cliente que pediu e enviar
        except:
            if (sock in self._clients.keys()):
                self._sel.unregister(sock)
                sock.close()
                for k in self._tree.keys():
                    if sock in self._tree[k]:
                        self._tree[k].remove(sock)
                del self._clients[sock]

        
        
    def list_topics(self) -> List[str]:
        """ Returns a list of strings containing all topics."""
        
        return self._topics

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        # a última mensagem do tópico
        if topic in self._store_values.keys():
            return self._store_values[topic]
        return None

    def put_topic(self, topic, value):
        """Store in topic the value."""
        if topic not in self._tree.keys():
            self._tree[topic] = []
        if topic not in self._topics:
            self._topics.append(topic)
        self._store_values[topic] = value

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        ret = []
        for t in self._tree.keys():
            if t in topic:
                for conn in self._tree.get(t):
                    ret.append((conn, self._clients[conn]))
        
        return ret

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        if topic not in self._tree.keys():
            self._tree[topic] = []
        if address not in self._clients.keys():
            self._clients[address] = _format
        self._tree[topic].append(address)

        msg = self.get_topic(topic)
        # message é um dicionário com todos os campos da mensagem
        # no get topic só estarão mensagens do tipo command = message
        if msg:  
            dic = {"command": "message", "topic": topic, "msg": msg}
            print("here")
            if _format == Serializer.JSON: 
                msg_enc = self.parse_msg_json(dic)  
                msg_len = len(msg_enc).to_bytes(2, byteorder='big')
                address.send(msg_len)
                address.send(msg_enc)
            elif _format == Serializer.PICKLE:
                msg_enc = self.parse_msg_pickle(dic)
                msg_len = len(msg_enc).to_bytes(2, byteorder='big')
                address.send(msg_len)
                address.send(msg_enc)
            elif _format == Serializer.XML:
                print("XML")
                msg_enc = self.parse_msg_xml(dic)
                print(msg_enc)
                msg_len = len(msg_enc).to_bytes(2, byteorder='big')   
                print (msg_len)
                address.send(msg_len)
                address.send(msg_enc)


    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        self._tree[topic].remove(address)
        del self._clients[address]

    def run(self):
        """Run until canceled."""
        
        while True:
            events = self._sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj)