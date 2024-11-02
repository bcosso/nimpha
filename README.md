Pet In-Memory database. Ideally it will be:
1) Distributed
2) able to perform changes in one node and replicate to others
3) Fully RSOCKET based
4)...

![alt text](Nimpha_Model.png)


TODO:
 - Update
 - Error treatment
 - Guarantee insert concurrency - Should be guaranteed by the elected WAL node as is
 - WAL node election mechanism
 - ~~Node unavailability treatment~~
 - Separate components in the code (refactor)
 - ~~Replication of WAL~~- WAL is replicated and on error (node down) triggers a data recovery event in one of the nodes
 - ~~Replication of data~~
 - Indexing mechanism (BTREE partially implemented)
 - ~~Sharding - Sharding strategy : TABLE~~
 - Sharding - Sharding strategies : Alphabetic order, index range, ...? 
 - ~~Convert from HTTP to a protocol lower in the OSI model~~ Done with RSOCKET
 - Remove non rsocket communication
 - refactor code to Golang standard naming convention. Started with snake case but had to change due to Golang standards.
 - ~~Drivers for usage by client systems~~ (Created HTTP load balancer. Clients should use post requests

TODO Parser:
 - INSERT
 - ~~select with subqueries~~
 - Select without where clause
 - Mathematical operations
 - ~~Case When~~
 - Nested Case When
 - ...
 
