## LSMT Aggregation

LSMT Aggregation represents database tables containing pre-aggregated data with user-defined aggregative functions.

* LSMT database Utilizes log-structured merge-tree data structure, so databases built on top of this table can easily 
handle high insert volumes of data (e.g. transactional logs).
* Aggregations can be stored on a distributed file system.