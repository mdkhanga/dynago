# dynago
A distributed key value store like Dynamo or Cassandra written in Go.

The inspiration comes from the Amazon Dynamo paper.  
https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

Current features :  
In memory Key value store  
Cluster - no leader all servers equal  
Gossip
Partitioning using consistent hashing  
 
In plan/ coming soon:
Quorum based read/write
Storage
Replication

## Usage

### Building the code

make

### Start a server

./dynago -i ip_addr_to_bind_to

-i ip address to bind server to

### Start a second server

./dynago -i ip_addr -p 8085 -h 8086 -seed ip_addr_of_seed:8081

-i ip address to bind server to
-p grpc port for server to server communication
-h http port for client communication (default 8080)
-seed another server to connect to

### Start a third server

./dynago -i ip_addr -p 8087 -h 8088 -seed ip_addr_of_seed:8081

### Get the list of members in the cluster

curl http://ip_addr:httpport/members

The request can be sent to any of the servers

### Store a Key/Value 

curl -X POST -H "Content-type:application/json" -d '{"Key": "Name", "Value":"somevalue"}' http://ip_addr:8080/kvstore

### Retrieve a the above value

curl http://ip_addr:8080/kvstore/Name

You can read any key from any node.

## Status 

### What is currently working ?

1. You can create a leader less peer to peer cluster by starting servers as described above. Except first server, the others need to point to another with the --seed option.

2. Set and Get Key values

Connect to any node and set key/value as shown above.

3. Distribution of keys across the nodes of the cluster using consistent hashing.

### Next up 

Uniform distribution of keys using consistent hashing.   

We have a dynamic leaderless cluster to which you can add or remove servers.   
With consistent hashing, we have a scalable in-memory key value store.


