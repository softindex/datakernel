## LSMT Database

LSMT Database is a log-structured merge-tree database designed for processing massive partial aggregations of 
raw data and forms a multidimensional OLAP (online analytical processing). It utilizes [Cloud-OT](https://github.com/softindex/datakernel/tree/master/cloud-ot) 
and [Cloud-FS](https://github.com/softindex/datakernel/tree/master/cloud-fs) technologies. LSMT database is truly 
asynchronous and distributed, with full support of transaction semantics. Dimension here can be treated as categories 
while measures represent values.

`Cube` class represents an OLAP cube. It provides methods for loading and querying data along with functionality for 
managing aggregations, such as:
 