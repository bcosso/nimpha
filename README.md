Pet In-Memory database. Ideally it will be:
1) Distributed
2) able to perform changes in one node and replicate to others
3) Fully HTTP based
4)...

![alt text](Nimpha_Model.png)


First load data
http://127.0.0.1:10000/traira/load_mem_table/

Select CONTAINS:
http://127.0.0.1:10000/traira/select_data_where_worker_contains?table=table1&where_field=name_client&where_content=teste

Insert:
http://127.0.0.1:10000/traira/insert_worker?table=table1&key_id=15
With body:
{"client_number":"15","name_client":"teste15"}

Delete CONTAINS:
http://127.0.0.1:10000/traira/delete_data_where?where_operator=contains&table=table1&where_field=name_client&where_content=teste7

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
 
