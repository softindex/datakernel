ETL is a near real-time async data processing system. Unlike traditional ETL(extraction-transformation-loading) systems, 
this module processes massive parallel Cluster-FS streams of data with its distributed worker servers and then commits 
partially processed results into commit graph of Cluster-OT. These commits are continuously merged into a single coherent 
result using merge and conflict resolution strategies provided by Cluster-OT. 

* ETL is used as a part of LSMT Database.