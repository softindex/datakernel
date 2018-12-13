LSMT database is an LSM-tree (log-structured merge-tree) database designed for processing massive partial aggregations of 
raw data and forms a multidimensional OLAP (online analytical processing). It utilizes [Cluster-OT](ot.html) and [Cluster-FS](fs.html) 
technologies. LSMT database is truly asynchronous and distributed, with full support of transaction semantics. 
Dimension here can be treated as categories while measures represent values.


 