LSMT Table Module provides the foundation for building almost any type of database by providing the possibility to 
define custom aggregation functions.

* LSMT database Utilizes log-structured merge-tree data structure, so databases built on top of this table can easily 
handle high insert volumes of data (e.g. transactional logs).
* Aggregations can be stored on a distributed file system.