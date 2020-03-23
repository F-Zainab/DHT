"""
CPSC 5520, Seattle University
:Authors: Fariha Zainab
:Version: f19-02
:Assignment: Lab4 - DHT-CHORD
"""
"""
Takes the existing node and key i.e (playerID, year), to get the details 
Example Query command for windows: py chord_query.py 50672 breezyreid/2523928 1952
"""
import sys
import socket
import csv
import pickle

BUFFER_SIZE = 4096
ADD_OR_UPDATE_ENTRY_REQUEST = 'ADD_OR_UPDATE_ENTRY_REQUEST'
GET_ENTRY_REQUEST = 'GET_ENTRY_REQUEST'

RPC_ARG_REQUEST_TYPE = 'RPC_ARG_REQUEST_TYPE'
RPC_ARG_KEY = 'RPC_ARG_KEY'
RPC_ARG_VALUE = 'RPC_ARG_VALUE'

class ChordQuery(object):

    def __init__(self, nodePort):
        self.nodeAddress = ('localhost', nodePort)

    def CreateClientRequestSocket(self):
        """
        start a client socket to handle the requests
        """
        requestSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        requestSocket.setblocking(True)
        requestSocket.connect(self.nodeAddress)
        return requestSocket

    def RemoteCall(self, argDict):
        """
        Handle remote call for the query
        :param argDict: argument dictionary to be sent to the remote node
        """
        requestSocket = self.CreateClientRequestSocket()
        requestByteArray = pickle.dumps(argDict)
        requestSocket.sendall(requestByteArray)
        requestSocket.shutdown(socket.SHUT_WR)
        value = pickle.loads(requestSocket.recv(BUFFER_SIZE))
        requestSocket.shutdown(socket.SHUT_RD)
        requestSocket.close()
        return value

    def AddOrUpdateEntry(self, key, value):
        """
        Create argument Dictionary for adding or updating the key
        :param key: (playerId and year)
        :param value: details for the key 
        """
        argDict = {RPC_ARG_REQUEST_TYPE: ADD_OR_UPDATE_ENTRY_REQUEST, RPC_ARG_KEY: key, RPC_ARG_VALUE: value}
        self.RemoteCall(argDict)

    def GetEntry(self, key):
        """
        Create argument dictionary to query a key in the chord network
        """
        argDict = {RPC_ARG_REQUEST_TYPE: GET_ENTRY_REQUEST, RPC_ARG_KEY: key}
        return self.RemoteCall(argDict)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Please enter the port number of existing node, player id and year to look up player details.")
        exit(1)   

    port = int(sys.argv[1])
    playerId = sys.argv[2]
    year = sys.argv[3]

    query = ChordQuery(port)
    key = (playerId, year)
    value = query.GetEntry(key)
    print(f'Key={key}, RetrievedValue={value}')
