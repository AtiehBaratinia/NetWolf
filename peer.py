"""
@author: Atieh Barati Nia- 9631010

ATTENTION: three folders with default files are available in this project for testing if you want!

"""

import random
import socket
import threading
import time
import os


class ServerUDP(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        print("server udp started")
        """ Listen for incoming datagrams"""
        while True:

            bytesAddressPair = UDP_server_socket.recvfrom(bufferSize)
            """address of the client is in its message, so we don't need to get address"""
            message = bytesAddressPair[0].decode()
            """if the message starts with discovery, it means it shares its list"""
            if message.startswith("discovery"):

                pieces = message[10:].split(" ")
                nodes = []
                i = 0
                while i < len(pieces) - 1:
                    nodes.append(Node(pieces[i], pieces[i + 1], int(pieces[i + 2])))
                    i += 3

                """ merge the received discovery list with its own discovery list"""
                for nodeGet in nodes:
                    flag = True
                    for node in discoveryList:
                        if node.name == nodeGet.name:
                            flag = False
                            break
                    if flag and nodeGet.name != self_name:
                        discoveryList.append(nodeGet)

                # for i in discoveryList:
                #     print(i.name, i.ip, int(i.port))

                """if the message starts with get, it means it asks whether the node have file or not"""
            elif message.startswith("get") or message.startswith("Get"):
                data = message.split(" ")
                file_name = data[1]
                if file_name in fileList:
                    target_ip = data[2]
                    target_udp_port = int(data[3])

                    """ it is for free ride, like said in project
                     if requesting peer has not give this peer files before then we add 2 sec for delays"""
                    for i in discoveryList:
                        if i.ip == target_ip and i.port == target_udp_port:
                            target_name = i.name
                            if target_name not in prior_communication:
                                time.sleep(TIME_FOR_FREE_RIDE)
                            break

                    send_message = "file " + self_name + " " + file_name + " " + self_ip + " " + str(self_tcpPORT)
                    UDPClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    UDPClientSocket.sendto(str.encode(send_message), (target_ip, target_udp_port))
                """if the message starts with file, it means the node is requesting a file"""
            elif message.startswith("file"):
                data = message.split(" ")
                peerName = data[1]
                file_name = data[2]
                target_ip = data[3]
                target_tcp_port = int(data[4])
                answers_for_get_file.append(AnswerGet(peerName, file_name, target_ip, target_tcp_port))
                # print(message)

            else:
                print("try again!!!")


class MultiSeverTCP(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        """ start tcp server"""
        TCP_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        flag = True
        """ generate random tcp server port, if the port was assigned before 
        then we generate another random port number"""
        while flag:
            try:
                global self_tcpPORT
                self_tcpPORT = random.randint(2000, 50000)
                TCP_server_socket.bind(('0.0.0.0', self_tcpPORT))
                flag = False
            except Exception:
                flag = True

        print("Server TCP started")
        print("Waiting for client request..")
        """with any request for tcp connection, a thread will be created"""
        while True:
            TCP_server_socket.listen(1)
            clientTCPsock, clientTCPAddress = TCP_server_socket.accept()
            newthread = ServerTCP(clientTCPAddress, clientTCPsock)
            newthread.start()


class ServerTCP(threading.Thread):

    def __init__(self, clientAddress, clientsocket):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        self.clientTCPAddress = clientAddress
        # print("New connection TCP added: ", clientAddress)

    def run(self):
        # print("TCP Connection from : ", self.clientTCPAddress)

        data = self.csocket.recv(bufferSize)
        msg = data.decode()
        complete_name = os.path.join(path_folder, msg)
        """opening the requested file and sending the contents to the peer"""
        f = open(complete_name, 'rb')
        l = f.read(bufferSize)
        while l:
            self.csocket.send(l)
            l = f.read(bufferSize)
        f.close()
        self.csocket.shutdown(socket.SHUT_WR)

        self.csocket.close()


class SendDiscovery(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            time.sleep(TIME_INTERVAL_DISCOVERY)
            """ send "discovery" which means the message is about discovery
            the protocol is <discovery NAME_PEER IP_PEER PORT_UDP_SERVER_PEER>"""
            send_list = "discovery"
            for i in discoveryList:
                temp = " " + i.name + " " + i.ip + " " + str(i.port)
                send_list += temp

            """Besides discovery list, sending the name, ip and udp_server_port to other peers """
            send_list += " " + self_name + " " + self_ip + " " + str(self_udp_port)

            for node in discoveryList:
                UDPClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                UDPClientSocket.sendto(str.encode(send_list), (node.ip, node.port))


""" A thread to send request for "get file" to all peers"""


class SendRequest(threading.Thread):
    def __init__(self, order):
        threading.Thread.__init__(self)
        self.order = order

    def run(self):
        for j in discoveryList:
            UDPClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            """the protocol to ask for a file is<get FILE_NAME SELF_IP SELF_UDP_PORT>"""
            data = self.order + " " + self_ip + " " + str(self_udp_port)
            UDPClientSocket.sendto(str.encode(data), (j.ip, j.port))
        time.sleep(WAIT_TIME_FOR_ANS_GET)
        file_name = self.order.split(" ")[1]

        list_requested_file = []
        for i in answers_for_get_file:
            if i.file_name == file_name:
                list_requested_file.append(i)

        if not list_requested_file:
            print("your requested file is not in the cluster")
            return

        for j in answers_for_get_file:
            """ the first item in the list which has the same file_name has the best response time
            because it was added first"""
            if j.file_name == file_name:
                """ send request to get file"""
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((j.target_ip, j.target_tcp_server_port))
                s.send(str.encode(file_name))

                complete_name = os.path.join(path_folder, file_name)
                f = open(complete_name, 'wb')

                for i in discoveryList:
                    if i.ip == j.target_ip and i.name == j.peerName:
                        print("getting ", file_name, "from node", i.name)
                        if i.name not in prior_communication:
                            prior_communication.append(i.name)
                        break

                l = s.recv(bufferSize)
                while l:
                    f.write(l)
                    l = s.recv(bufferSize)
                f.close()
                fileList.append(file_name)

                s.close()


""" object for saving properties of peers"""
class Node:
    """
    UDPport is for UDP_server_port
    """
    def __init__(self, name, ip, UDPport: int):
        self.name = name
        self.ip = ip
        self.port = UDPport


"""object for the answers coming from other nodes in response to get file request"""
class AnswerGet:
    def __init__(self, peerName, file_name, target_ip, target_tcp_server_port):
        self.peerName = peerName
        self.file_name = file_name
        self.target_ip = target_ip
        self.target_tcp_server_port = target_tcp_server_port


discoveryList = []
prior_communication = []
fileList = []
answers_for_get_file = []
bufferSize = 2048
self_tcpPORT = 0

TIME_INTERVAL_DISCOVERY = 3
WAIT_TIME_FOR_ANS_GET = 5
TIME_FOR_FREE_RIDE = 2

if __name__ == "__main__":

    self_ip = socket.gethostbyname(socket.gethostname())
    print(self_ip)
    self_name = input("what is your name?\n")
    self_udp_port = int(input("what is your udp server port?\n"))

    path_folder = input("where is your folder name?(enter complete path)\n")
    fileList = os.listdir(path_folder)

    print("do you want to enter discovery list? yes:1 / no:2")
    haveDiscovery = input()
    while haveDiscovery != '1' and haveDiscovery != '2':
        print("not accepted! try again!")
        haveDiscovery = input("do you want to enter discovery list? yes:1 / no:2 \n")

    if haveDiscovery == '1':
        name = ''
        while True:
            name = input("enter name of the peer:(if you are finished, enter <end>) ")
            if name == "end":
                break
            ip = input("enter ip of the peer: ")
            port = int(input("enter udp server port ot the peer: "))
            discoveryList.append(Node(name, ip, port))

    """ start udp server"""
    UDP_server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    UDP_server_socket.bind(('0.0.0.0', self_udp_port))
    udpServerThread = ServerUDP()
    udpServerThread.start()

    """ start tcp server"""
    multiTCPServer = MultiSeverTCP()
    multiTCPServer.start()

    """ start discovery service"""
    discoveryService = SendDiscovery()
    discoveryService.start()

    """ communication with user"""
    while True:
        order = input("what do you want?\nif you want to see other peers enter <list>\n"
                      "if you want to get a file enter <get FILE_NAME>\n")
        if order == "list":
            for i in discoveryList:
                print(i.name, i.ip, i.port)
        elif order.startswith("get") or order.startswith("Get"):
            flag = True
            temp = order.split(" ")[1]
            if temp in fileList:
                flag = False
                print("You already have this file!")
            if flag:
                getReq = SendRequest(order)
                getReq.start()
