## ETL

ETL is a near real-time async data processing system. Unlike traditional ETL(extraction-transformation-loading) systems, 
this module processes massive parallel [FS](https://github.com/softindex/datakernel/tree/master/cloud-fs) streams of 
data with its distributed worker servers and then commits partially processed results into commit graph of 
[OT](https://github.com/softindex/datakernel/tree/master/cloud-ot). These commits are continuously merged into a single 
coherent result using merge and conflict resolution strategies provided by OT. 

* ETL is used as a part of [LSMT Database](https://github.com/softindex/datakernel/tree/master/cloud-lsmt-db).