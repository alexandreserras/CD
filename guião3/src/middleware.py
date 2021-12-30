"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
import selectors
import socket
import json
import pickle
import xml.etree.ElementTree as ET
from src.broker import Serializer
from typing import Any

class MiddlewareType(Enum):
    """Middleware Type."""
    CONSUMER = 1
    PRODUCER = 2

class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER, _method=Serializer.JSON):
        """Create Queue."""
        self._topic = topic
        self._type = _type
        self._method = _method

        self._sock = socket.socket()
        self._host = "localhost"
        self._port = 5000
        self._sock.connect((self._host, self._port))
        # self._sock.setblocking(True)
        print(f"Trying to connect to {self._host} at port {self._port}")

    def reg_msg(self):        
        msg={"command": "register", "topic":  self._topic, "type": self._type.value, "method": self._method.value}
        # IMPORTANTE: Depois do register message temos de mandar uma de subscribe
        msg_enc = json.dumps(msg).encode("utf-8")
        msg_len = len(msg_enc).to_bytes(2, byteorder='big')     
        self._sock.send(msg_len)
        self._sock.send(msg_enc)

    def push(self, value):
        """Sends data to broker. """
        print(value)
        msg_len = len(value).to_bytes(2, byteorder='big')             
        self._sock.send(msg_len)
        self._sock.send(value)
        

    def pull(self) -> (str, Any): # DONE
        """Waits for (topic, data) from broker.
        Should BLOCK the consumer!"""
        pass

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        if (self._method == Serializer.JSON):
            msg={"command": "list_all"}
            msg_enc = json.dumps(msg).encode("utf-8")
            self.push(msg_enc)
            
        elif (self._method == Serializer.PICKLE):
            msg={"command": "list_all"}
            msg_enc = pickle.dumps(msg)
            self.push(msg_enc)

        elif (self._method == Serializer.XML):
            data = "<data><command>"+str("list_all")+"</command><topic>"
            data = data+"</data>"
            data = ET.fromstring(data)
            msg_enc = ET.tostring(data, 'utf-8')
            self.push(msg_enc)
        

    def cancel(self):
        """Cancel subscription."""
        if (self._method == Serializer.JSON):
            msg = {"command" : "unsubscribe","topic": self._topic}
            msg_enc = json.dumps(msg).encode("utf-8")
            self.push(msg_enc)

        elif (self._method == Serializer.PICKLE):
            msg = {"command" : "unsubscribe","topic": self._topic}
            msg_enc = pickle.dumps(msg)
            self.push(msg_enc)

        elif (self._method == Serializer.XML):
            data = "<data><command>"+str("unsubscribe")+"</command><topic>"+str(self._topic)+"</topic>"
            data = data+"</data>"
            data = ET.fromstring(data)
            msg_enc = ET.tostring(data, 'utf-8')
            self.push(msg_enc)
    
class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create JSON Queue."""
        print(f"topic: {topic}")
        super().__init__(topic, _type, Serializer.JSON)
        super().reg_msg()

        if self._type == MiddlewareType.CONSUMER:
            print("vou subscrever")
            self.subscribe()

    def push(self, value):
        """Sends data to broker. """
        # print(f"value: {value}")
        msg = {"command": "message", "topic": self._topic, "msg": value}
        msg_enc = json.dumps(msg).encode("utf-8")
        super().push(msg_enc)

    def subscribe(self):
        msg = {"command" : "subscribe", "topic": self._topic}
        msg_enc = json.dumps(msg).encode("utf-8")
        super().push(msg_enc)
    
    def pull(self) -> (str, Any):
        """Waits for (topic, data) from broker.
        Should BLOCK the consumer!"""
        msg_header = self._sock.recv(2)
        if not len(msg_header):
            return False
        msg_len = int.from_bytes(msg_header, byteorder='big')
        msg_dec = self._sock.recv(msg_len).decode("utf8")
        ret = json.loads(msg_dec)
        print(ret)
        return (ret["topic"],ret["msg"])

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create XML Queue."""
        print(f"topic: {topic}")
        super().__init__(topic, _type, Serializer.XML)
        super().reg_msg()
        
        if self._type == MiddlewareType.CONSUMER:
            print("vou subscrever")
            self.subscribe()

    def push(self, value):
        """Sends data to broker. """  
        root = ET.Element('data')
        ET.SubElement(root, "command").set("value", "message")
        ET.SubElement(root, "topic").set("value", str(self._topic))
        ET.SubElement(root, "msg").set("value", str(value))
        msg_enc = ET.tostring(root, 'utf-8')
        super().push(msg_enc)

    def subscribe(self):
        root = ET.Element('data')
        ET.SubElement(root, "command").set("value", "subscribe")
        ET.SubElement(root, "topic").set("value", str(self._topic))
        msg_enc = ET.tostring(root, 'utf-8')
        super().push(msg_enc)
    
    def pull(self) -> (str, Any):
        """Waits for (topic, data) from broker.
        Should BLOCK the consumer!"""
        print("entrei")
        msg_header = self._sock.recv(2)
        print(msg_header)
        if not len(msg_header):
            print("ret false")
            return False
        msg_len = int.from_bytes(msg_header, byteorder='big')
        msg_dec = self._sock.recv(msg_len).decode("utf8")
        ret = ET.fromstring(msg_dec)
        print(ret)
        dic_ret = {}
        for child in ret:
            dic_ret[child.tag] = child.attrib["value"]
        print(dic_ret)
        return (dic_ret["topic"], dic_ret["msg"])

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Pickle Queue."""
        print(f"topic: {topic}")
        super().__init__(topic, _type, Serializer.PICKLE)
        super().reg_msg()

        if self._type == MiddlewareType.CONSUMER:
            print("vou subscrever")
            self.subscribe()

    def push(self, value):
        """Sends data to broker. """
        msg = {"command":"message","topic":self._topic,"msg": value}
        msg_enc = pickle.dumps(msg)
        super().push(msg_enc)

    def subscribe(self):
        msg = {"command" : "subscribe", "topic": self._topic}
        msg_enc = pickle.dumps(msg)
        super().push(msg_enc)

    def pull(self) -> (str, Any):
        """Waits for (topic, data) from broker.
        Should BLOCK the consumer!"""
        msg_header = self._sock.recv(2)
        if not len(msg_header):
            return False
        msg_len = int.from_bytes(msg_header, byteorder='big')
        msg_dec = self._sock.recv(msg_len)
        ret = pickle.loads(msg_dec)
        print(ret)
        return (ret["topic"],ret["msg"])