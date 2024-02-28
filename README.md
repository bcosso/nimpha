Pet In-Memory database. Ideally it will be:
1) Distributed
2) able to perform changes in one node and replicate to others
3) Fully RSOCKET based
4)...

![alt text](Nimpha_Model.png)


TODO:
 - Update
 - Error treatment
 - Node unavailability treatment
 - Separate components in the code (refactor)
 - Replication of WAL
 - Replication of data
 - ~~Convert from HTTP to a protocol lower in the OSI model~~ Done with RSOCKET
 - Remove non rsocket communication
 - refactor code to Golang standard naming convention. Started with snake case but had to change due to Golang standards.
 
