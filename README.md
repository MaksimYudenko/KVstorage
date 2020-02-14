***

### Distributed K-V storage

***

This program is a web application which capabilities are:	

* store entities matching the pattern [key:value] 
* obtaining objects from web request and further converting to a json file
* keeping is carried out by saving entities in a separate database depending on 
the key's hash
* each database is on the remote node machine
* each node corresponds to an IP address, the properties are in the 
nodeProps.json file
* nodes are compiled into groups: in case of problems with a node a replication 
mechanism provided to prevent any CRUD operation issues (round-robin algorithm)
* node distribution is configured in the nodesGroup.json file
* each operation with database is processed through a transaction
* data caching is provided: LFU or LRU, depends on preferences which sets 
in appropriate field when setting up the collection object

***

### Used technologies

* RESTful API
* Spring Boot / MVC / Data
* PostgreSQL
* Hibernate
* Log4j
* Mockito
* Gradle

***