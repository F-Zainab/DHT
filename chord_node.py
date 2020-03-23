"""
CPSC 5520, Seattle University
:Authors: Fariha Zainab
:Version: f19-02
:Assignment: Lab4 - DHT-CHORD
"""
"""
Start the chord_node by giving the port no. 0 to start the network
The node will print it's finger table, its address and predecessor.
When the next node joins the network, it can take the port no. from the address printed by previous nodes
"""

import sys
import socket
import csv
import hashlib
import pickle
import selectors
import threading

M_BITS = 5#160
NODES = 2**M_BITS

FIND_SUCCESSOR_REQUEST = "FIND_SUCCESSOR_REQUEST"
GET_PREDECESSOR_REQUEST = "GET_PREDECESSOR_REQUEST"
GET_SUCCESSOR_REQUEST = "GET_SUCCESSOR_REQUEST"
CLOSEST_PRECEDING_FINGER_REQUEST = "CLOSEST_PRECEDING_FINGER_REQUEST"
UPDATE_FINGER_TABLE_REQUEST = "UPDATE_FINGER_TABLE_REQUEST"
ADD_OR_UPDATE_ENTRY_REQUEST = "ADD_OR_UPDATE_ENTRY_REQUEST"
GET_ENTRY_REQUEST = 'GET_ENTRY_REQUEST'
LOCAL_ADD_OR_UPDATE_ENTRY_REQUEST = "LOCAL_ADD_OR_UPDATE_ENTRY_REQUEST"
LOCAL_GET_ENTRY_REQUEST = 'LOCAL_GET_ENTRY_REQUEST'

RPC_ARG_REQUEST_TYPE = 'RPC_ARG_REQUEST_TYPE'
RPC_ARG_NODE_INFO = 'RPC_ARG_NODE_INFO'
RPC_ARG_KEY = 'RPC_ARG_KEY'
RPC_ARG_VALUE = 'RPC_ARG_VALUE'
RPC_ARG_ID = 'RPC_ARG_ID'
RPC_ARG_INDEX = 'RPC_ARG_INDEX'

BUFFER_SIZE = 4096
BACKLOG = 100


class NodeInfo(object):
    """
    class for storing node related information i.e. address and hash value
    """
    def __init__(self, hashValue, address):
        self.HashValue = hashValue
        self.Address = address

class FingerTableEntry(object):
    """
    Class for the finger table of the node
    """
    def __init__(self):
        self.Start = None
        self.Interval = None
        self.Node = NodeInfo(None, None)

class ChordNode(object):
    """
    class for handling all the functionalities of the node
    """

    def __init__(self):
        self.fingerTable = {}
        self.localData = {}
        self.predecessor = None
        self.nodeHashValue = None
        self.selfNodeAddress = None
        self.nodeInfo = None
        self.listenSocket = None
        self.initialNodeAddress = None
        self.hasJoined = False
        for i in range(1, M_BITS+1):
            self.fingerTable[i] = FingerTableEntry()

    def GetHashKey(self, key):
        """
        Using SHA-1 for creating a hash value for the object
        :param key: object we want to create hash value for
        """
        data = pickle.dumps(key)
        hashObject = hashlib.sha1(data)
        hashValue = hashObject.hexdigest()
        value = int(hashValue, 16)
        return value

    def CreateListenSocket(self):
        """
        Create the server socket for the node
        """
        self.listenSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listenSocket.bind(('localhost', 0))
        self.listenSocket.listen(BACKLOG)
        self.listenSocket.setblocking(False)
        return self.listenSocket

    def WaitForRequest(self):
        """
        Creating thread for each request
        """
        selector = selectors.DefaultSelector()
        selector.register(self.listenSocket, selectors.EVENT_READ)
        while True:
            events = selector.select(timeout = 10)
            for __, __ in events:

                self.listenSocket.setblocking(True)
                sock, address = self.listenSocket.accept()
                self.listenSocket.setblocking(False)                
                bgThread = threading.Thread(target=self.HandleRemoteCall, args=(sock, address))
                bgThread.start()

    def HandleRemoteCall(self, sock, address):
        """
        Handle the RPC calls coming from different nodes
        """
        sock.setblocking(True)
        rpcArgs = pickle.loads(sock.recv(BUFFER_SIZE))

        print(f"Received RPC for {rpcArgs[RPC_ARG_REQUEST_TYPE]} to {address}")

        value = {}
        if(rpcArgs[RPC_ARG_REQUEST_TYPE] == FIND_SUCCESSOR_REQUEST):
            value = self.FindSuccessor(rpcArgs[RPC_ARG_ID])
        elif (rpcArgs[RPC_ARG_REQUEST_TYPE] == GET_PREDECESSOR_REQUEST):
            remoteNodePredecessor = self.predecessor
            self.predecessor = rpcArgs[RPC_ARG_NODE_INFO]
            value = remoteNodePredecessor
        elif rpcArgs[RPC_ARG_REQUEST_TYPE] == GET_SUCCESSOR_REQUEST:
            value = self.fingerTable[1].Node
        elif rpcArgs[RPC_ARG_REQUEST_TYPE] == CLOSEST_PRECEDING_FINGER_REQUEST:
            value = self.ClosestPrecedingFinger(rpcArgs[RPC_ARG_ID])
        elif rpcArgs[RPC_ARG_REQUEST_TYPE] == UPDATE_FINGER_TABLE_REQUEST:
            self.UpdateFingerTable(rpcArgs[RPC_ARG_INDEX], rpcArgs[RPC_ARG_NODE_INFO])
        elif rpcArgs[RPC_ARG_REQUEST_TYPE] == ADD_OR_UPDATE_ENTRY_REQUEST:
            self.AddOrUpdateEntry(rpcArgs[RPC_ARG_KEY], rpcArgs[RPC_ARG_VALUE])
        elif rpcArgs[RPC_ARG_REQUEST_TYPE] == GET_ENTRY_REQUEST:
            value = self.GetEntry(rpcArgs[RPC_ARG_KEY])
        elif rpcArgs[RPC_ARG_REQUEST_TYPE] == LOCAL_ADD_OR_UPDATE_ENTRY_REQUEST:
            self.LocalAddOrUpdateEntry(rpcArgs[RPC_ARG_KEY], rpcArgs[RPC_ARG_VALUE])
        elif rpcArgs[RPC_ARG_REQUEST_TYPE] == LOCAL_GET_ENTRY_REQUEST:
            value = self.LocalGetEntry(rpcArgs[RPC_ARG_KEY])

        sock.sendall(pickle.dumps(value))
        self.ShutDownSocket(sock)

    def CreateAClientSocket(self, address):
        """
        creating the client socket of the node to send request 
        :param address: address of the node on which request is to be sent
        """
        requestSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        requestSocket.setblocking(True)
        requestSocket.connect(address)
        return requestSocket
    
    def RemoteCall(self, address, argDict):
        """
        Making the RPC call to the nodes
        :param address: address of the node on which the node will make an RPC
        :param argDict: dictionary contaiining all the data to be sent for processing on the node 
        """
        print(f"Making RPC for {argDict[RPC_ARG_REQUEST_TYPE]} to {address}")
        requestSocket = self.CreateAClientSocket(address)
        requestByteArray = pickle.dumps(argDict)
        requestSocket.sendall(requestByteArray)
        requestSocket.shutdown(socket.SHUT_WR)
        value = pickle.loads(requestSocket.recv(BUFFER_SIZE))
        requestSocket.shutdown(socket.SHUT_RD)
        requestSocket.close()
        return value

    def ShutDownSocket(self, s):
        """
        Utility function to shut down the socket
        """
        try:
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        except socket.error:
            pass

    def GetEntry(self, key):
        """
        Handle the query for the key
        :param key: key value queried by the user
        """
        keyId = self.GetHashKey(key)
        succNode = self.FindSuccessor(keyId)
        return self.RemoteGetEntry(succNode.Address, key)

    def AddOrUpdateEntry(self, key, value):
        """
        Handle the add or update of the key and values in the chord network
        """
        keyId = self.GetHashKey(key)
        succNode = self.FindSuccessor(keyId)
        self.RemoteAddOrUpdateEntry(succNode.Address, key, value)

    def LocalAddOrUpdateEntry(self, key, value):
        """
        add or update the keys and values in the node locally
        """
        self.localData[key] = value

    def LocalGetEntry(self, key):
        """
        local method to get the value for the key queried by the user
        """
        if key in self.localData:
            return self.localData[key]
        return None

    def RemoteAddOrUpdateEntry(self, destAddress, key, value):
        """
        Handle the remote call for adding and updating the entry in the chord network
        :param destAddress: address of the node to make an RPC call
        :param key: key to be added
        :param value: value related to that key
        """
        if(destAddress == self.selfNodeAddress):
            self.LocalAddOrUpdateEntry(key, value)
        argDict = {}
        argDict[RPC_ARG_REQUEST_TYPE] = LOCAL_ADD_OR_UPDATE_ENTRY_REQUEST
        argDict[RPC_ARG_KEY] = key
        argDict[RPC_ARG_VALUE] = value
        self.RemoteCall(destAddress, argDict)

    def RemoteGetEntry(self, destAddress, key):
        """
        Handles remote call to get the value of entry queried by the user
        :param destAddress: address of the node to make an RPC call
        :param key: key to be queried
        """
        if(destAddress == self.selfNodeAddress):
            return self.LocalGetEntry(key)
        argDict = {}
        argDict[RPC_ARG_REQUEST_TYPE] = LOCAL_GET_ENTRY_REQUEST
        argDict[RPC_ARG_KEY] = key
        return self.RemoteCall(destAddress, argDict)

    def RemoteFindSuccessor(self, destAddress, id):
        """
        Handles the remote call to find the successor of the node
        :param destAddress: address of the remote note
        :param id: id of the node, we want to know the successor node for
        """
        if(destAddress == self.selfNodeAddress):
            value = self.FindSuccessor(id)
            return value
        argDict = {}
        argDict[RPC_ARG_REQUEST_TYPE] = FIND_SUCCESSOR_REQUEST
        argDict[RPC_ARG_ID] = id
        successor = self.RemoteCall(destAddress, argDict)
        return successor

    def RemoteGetAndSetPredecessor(self, destAddress, node):
        """
        Handles remote call to get and set the predecessor for the node
        :param destAddress: address of the remote note
        :param node: node details we want to know the predecessor of
        """
        if(destAddress == self.selfNodeAddress):
            return self.predecessor
        argDict = {}
        argDict[RPC_ARG_REQUEST_TYPE] = GET_PREDECESSOR_REQUEST
        argDict[RPC_ARG_NODE_INFO] = node
        predecessor = self.RemoteCall(destAddress, argDict)
        return predecessor

    def RemoteGetSuccessor(self, destAddress):
        """
        Handles remote call to get the successor of the given node
        :param destAddress: address of the remote note
        """
        if (destAddress == self.selfNodeAddress):
            return self.fingerTable[1].Node
        argDict = {}
        argDict[RPC_ARG_REQUEST_TYPE] = GET_SUCCESSOR_REQUEST
        successor = self.RemoteCall(destAddress, argDict)
        return successor

    def RemoteClosestPrecedingFinger(self, destAddress, id):
        """
        Handles remote call to find the closest preceding node for the given node
        :param destAddress: address of the remote note
        :param id: id of the node we want the closest preceding finger for
        """
        if(destAddress == self.selfNodeAddress):
            value = self.ClosestPrecedingFinger(id)
            return value
        argDict = {}
        argDict[RPC_ARG_ID] = id
        argDict[RPC_ARG_REQUEST_TYPE] = CLOSEST_PRECEDING_FINGER_REQUEST
        precedingFinger = self.RemoteCall(destAddress, argDict)
        return precedingFinger

    def RemoteUpdateFingerTable(self, destAddress, fingerTableIndex, node):
        """
        Handles remote call to update the finger table of all the nodes that have the entry for the given node
        :param destAddress: address of the remote note
        :param fingerTableIndex: row number of teh finger table
        :param node: the details of the given node
        """
        if(destAddress == self.selfNodeAddress):
            self.UpdateFingerTable(fingerTableIndex, node)
        argDict = {}
        argDict[RPC_ARG_REQUEST_TYPE] = UPDATE_FINGER_TABLE_REQUEST
        argDict[RPC_ARG_INDEX] = fingerTableIndex
        argDict[RPC_ARG_NODE_INFO] = node
        self.RemoteCall(destAddress, argDict)

    def InitFingerTable(self, initialNodeAddress):
        """
        Method to intialise the finger table for a given node when it joins the chord network
        :param initialNodeAddress: the address of the chord node used by this node to join the network
        """
        self.fingerTable[1].Node =  self.RemoteFindSuccessor(initialNodeAddress, self.fingerTable[1].Start)
        self.predecessor = self.RemoteGetAndSetPredecessor(self.fingerTable[1].Node.Address, self.nodeInfo)
        for i in range(1, M_BITS):
            if self.IsInRange(self.fingerTable[i+1].Start, self.nodeInfo.HashValue, True, self.fingerTable[i].Node.HashValue, False):
                self.fingerTable[i+1].Node = self.fingerTable[i].Node
            else:
                node = self.RemoteFindSuccessor(initialNodeAddress, self.fingerTable[i+1].Start)
                self.fingerTable[i+1].Node =  node

    def CreateFingerTable(self):
        """
        Create the finger table for the node
        """
        for i in range(1, M_BITS+1):
            self.fingerTable[i].Start = ((self.nodeHashValue + (2**(i-1))) % NODES)
        for i in range(1, M_BITS+1):
            if(i < M_BITS):
                self.fingerTable[i].Interval = range(self.fingerTable[i].Start, self.fingerTable[i+1].Start)
            else:
                lastIntervalEntry = (self.nodeHashValue + 2**M_BITS)% 2**M_BITS
                self.fingerTable[i].Interval = range(self.fingerTable[i].Start, lastIntervalEntry)
    
    def FindSuccessor(self, id):
        """
        Local method to find the successor of the node 
        :param id: id of the node we want the successor of
        """
        node = self.FindPredeccesor(id)
        return self.RemoteGetSuccessor(node.Address)
    
    def FindPredeccesor(self, id):
        """
         Local method to find the predecessor of the node 
        :param id: id of the node we want the predecessor of
        """
        node = self.nodeInfo
        while True:
            succNode = self.RemoteGetSuccessor(node.Address)
            if self.IsInRange(id, node.HashValue, False,succNode.HashValue, True) == False:
                node = self.RemoteClosestPrecedingFinger(node.Address, id)
            else:
                break
        return node

    def ClosestPrecedingFinger(self, id):
        """
        Local method to find the closest preceding finger of the node
        :param id: id of the node we want the closest preceding finger for
        """
        for i in range(M_BITS, 0, -1):
            if self.IsInRange(self.fingerTable[i].Node.HashValue, self.nodeInfo.HashValue, False, id, False):
                return self.fingerTable[i].Node
        return self.nodeInfo

    def UpdateOthers(self):
        """
        Local method to update the finger table entries
        """
        for i in range(1, M_BITS+1):
            predNode = self.FindPredeccesor((1 + self.nodeHashValue - 2**(i-1) + NODES) % NODES)
            self.RemoteUpdateFingerTable(predNode.Address, i, self.nodeInfo)

    def UpdateFingerTable(self, i, s):
        """
        local call to update finger table entries
        """
        """ if s is i-th finger of n, update this node's finger table with s """
        ftEntry = self.fingerTable[i]
        
        # FIXME: don't want e.g. [1, 1) which is the whole circle
        # FIXME: bug in paper, [.start
        if (ftEntry.Start != ftEntry.Node.HashValue  and self.IsInRange(s.HashValue, ftEntry.Start, True, ftEntry.Node.HashValue, False)):  
            self.fingerTable[i].Node = s
            self.PrintFingerTable()
            self.RemoteUpdateFingerTable(self.predecessor.Address, i, s)

    def NodeJoin(self, host, port):
        """
        Method to join the node in the chord network
        """
        self.selfNodeAddress = self.listenSocket.getsockname()
        self.nodeHashValue = self.GetHashKey(self.selfNodeAddress)
        self.nodeInfo = NodeInfo(self.nodeHashValue, self.selfNodeAddress)
        self.initialNodeAddress = (host, port)
        self.CreateFingerTable()
        if(port == 0):
            for i in range(1, M_BITS+1):
                self.fingerTable[i].Node = self.nodeInfo
            self.predecessor = self.nodeInfo
        else:
            self.InitFingerTable(self.initialNodeAddress)
            self.UpdateOthers()
        
        self.hasJoined = True
        self.PrintFingerTable()
        print("Node Address is {}".format(self.selfNodeAddress))
        print("Node joined successfully.")

    def PrintFingerTable(self):
        """
        Method to print the finger table of the node
        """
        if self.hasJoined == False:
            return
        print("")
        print("---------------------------------------------")
        for __, v in self.fingerTable.items():
            print(f"Start={v.Start}, Interval={v.Interval}, Node=({v.Node.HashValue}, {v.Node.Address})")
        print("---------------------------------------------")
        print(f"Predecessor=({self.predecessor.HashValue}, {self.predecessor.Address})")

    def IsInRange(self, id,  start, isStartInclusive, end, isEndInclusive):
        """
        Utility method to manage the range 
        """
        if isStartInclusive == False:
            start = (start + 1) % NODES
        if isEndInclusive == True:
            end = (end + 1) % NODES
        allRanges = []
        if(start < end):
            allRanges.append(range(start, end))
        else:
            allRanges.append(range(start, NODES))
            allRanges.append(range(0, end))
        for r in allRanges:
            if id in r:
                return True
        return False

if __name__ == "__main__":
    #if len(sys.argv) != 2:
    #    print("Please enter the port number")
    #    exit(1)   

    host = 'localhost'
    port = 0#int(sys.argv[1])

    node = ChordNode()
    node.CreateListenSocket()
    bgThread = threading.Thread(target=node.NodeJoin, args=(host, port))
    bgThread.start()
    node.WaitForRequest()
