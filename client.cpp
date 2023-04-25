#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "SetMessage.h"

int main(int argc, char const *argv[]) {
    const char *serverIP = "127.0.0.1";
    int portNum = 8080;

    if (argc != 3) {
        std::cout << "Usage: client key value" << std::endl;
        return 1;
    }
    
    // create sockets
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (clientSocket == -1) {
        std::cerr << "Error creating socket" << std::endl;
        return 1;
    }
    
    // set up server address
    struct sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(portNum);
    serverAddr.sin_addr.s_addr = inet_addr(serverIP);
    
    // connect to server
    int status = connect(clientSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr));
    if (status == -1) {
        std::cerr << "Error connecting to server" << std::endl;
        return 1;
    }
    
    // send data to server
    // const char *message = "Hello, server!";
    char message[1024];

    // SetMessage s("key1","value1");
    std::string key(argv[1]);
    std::string value(argv[2]);
    SetMessage s(key, value);
    int bytes = s.serialize(message);


    // int numBytesSent = send(clientSocket, message, strlen(message), 0);
    int numBytesSent = send(clientSocket, message, bytes, 0);
    if (numBytesSent == -1) {
        std::cerr << "Error sending data to server" << std::endl;
        close(clientSocket);
        return 1;
    }
    
    // receive data from server
    char buffer[1024];
    int numBytesReceived = recv(clientSocket, buffer, 1024, 0);
    if (numBytesReceived == -1) {
        std::cerr << "Error receiving data from server" << std::endl;
        close(clientSocket);
        return 1;
    }
    buffer[numBytesReceived] = '\0';
    std::cout << "Received from server: " << buffer << std::endl ;

}
