"""
CPSC 5520, Seattle University
:Authors: Fariha Zainab
:Version: f19-02
:Assignment: Lab4 - DHT-CHORD
"""
"""
Whenever a node joins it prints it finger table.
Once the network is formed we run chord_populate by giving the port no. of any node  from the finger table in the chord network and 
name of the file in the command line argument 
"""
"""
NOTED OBESERVATIONS:
While running chord_populate file on my laptop the socket is running into error sometimes, but when I run it on my desktop computer
it is running smoothly. Probably it is because the laptop is not powerful enough to handle that many connections.
"""
import sys
import socket
import csv
import pickle
import time

BUFFER_SIZE = 4096
KEY_FIELD_PLAYER_ID = 'Player Id'
KEY_FIELD_YEAR = 'Year'
ADD_OR_UPDATE_ENTRY_REQUEST = 'ADD_OR_UPDATE_ENTRY_REQUEST'

RPC_ARG_REQUEST_TYPE = 'RPC_ARG_REQUEST_TYPE'
RPC_ARG_KEY = 'RPC_ARG_KEY'
RPC_ARG_VALUE = 'RPC_ARG_VALUE'

class ChordPopulator(object):

    def __init__(self, nodePort, dataFile):
        """
        :param nodePort: port of any node in the network
        :param dataFile: File used to populate the network 
        """
        self.nodeAddress = ('localhost', nodePort)
        self.dataFile = dataFile

    def CreateClientRequestSocket(self):
        """
        create the client socket for requests
        """
        requestSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        requestSocket.setblocking(True)
        requestSocket.connect(self.nodeAddress)
        return requestSocket

    def PopulateFromFile(self):
        """
        Method to read the file and populate the chord network
        """
        with open(self.dataFile, mode = "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                key = (row[KEY_FIELD_PLAYER_ID], row[KEY_FIELD_YEAR])
                value = row
                print(f"Populating entry for key={key}")
                self.PopulateEntry(key, value)


    def RemoteCall(self, argDict):
        """
        Handle the remote call to the node to populate the chord network
        :param argDict: dictionary containing the populate request and the data of the file
        """
        requestSocket = self.CreateClientRequestSocket()
        requestByteArray = pickle.dumps(argDict)
        requestSocket.sendall(requestByteArray)
        requestSocket.shutdown(socket.SHUT_WR)
        value = pickle.loads(requestSocket.recv(BUFFER_SIZE))
        requestSocket.shutdown(socket.SHUT_RD)
        requestSocket.close()
        return value

    def PopulateEntry(self, key, value):
        """
        Creating the argument dictionary
        """
        argDict = {RPC_ARG_REQUEST_TYPE: ADD_OR_UPDATE_ENTRY_REQUEST, RPC_ARG_KEY: key, RPC_ARG_VALUE: value}
        self.RemoteCall(argDict)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Please enter the port number of existing node and file name of data file.")
        exit(1)   

    port = int(sys.argv[1])
    dataFile = sys.argv[2]

    populator = ChordPopulator(port, dataFile)
    populator.PopulateFromFile()
    print('Entries from file populated successfully')

















