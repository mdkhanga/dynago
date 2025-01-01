# dynago
A distributed key value store like Dynamo or Cassandra written in Go.

The inspiration comes from the Amazon Dynamo paper.  
https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf

Plan:  
Key value store  
Cluster - no leader all servers equal  
Partitioning using consistent hashing  
Storage - SSTable  
Gossip  
etc      

WORK IN PROGRESS. NOT READY FOR USE.

## Usage

### Building the code

make

### Start a server

./kvstore

### Store a Key/Value 

curl -X POST -H "Content-type:application/json" -d '{"Key": "Name", "Value":"somevalue"}' http://localhost:8080/kvstore

### Retrieve a the above value

curl http://localhost:8080/kvstore/Name
