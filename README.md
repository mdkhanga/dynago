# dynago
A distributed key value store like Dynamo or Cassandra written in Go.

The inspiration comes from the Amazon Dynamo paper.  
https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

Plan:  
Key value store  
Cluster - no leader all servers equal  
Partitioning using consistent hashing  
Quorum based read / write  
Storage   TBD  
Gossip  
etc      

WORK IN PROGRESS. NOT READY FOR USE.

## Usage

### Building the code

make

### Start a server

./dynago -i ip_addr_to_bind_to

-i ip address to bind server to

### Start a second server

./dynago -i 192.168.1.14 -p 8085 -h 8086 -seed 192.168.1.14:8081

-i ip address to bind server to
-p grpc port for server to server communication
-h http port for client communication (default 8080)
-seed another server to connect to

### Start a third server

./dynago -i 192.168.1.14 -p 8087 -h 8088 -seed 192.168.1.14:8081

### Store a Key/Value 

curl -X POST -H "Content-type:application/json" -d '{"Key": "Name", "Value":"somevalue"}' http://ip_addr:8080/kvstore

### Retrieve a the above value

curl http://ip_addr:8080/kvstore/Name
