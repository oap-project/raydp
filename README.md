# RayDP: Distributed Data Processing on Ray

A distributed data processing tool on Ray. Currently, we support running Spark on Ray:

* Support startup Spark cluster on Ray environments. Now, we will startup a Spark standalone cluster, 
this maybe replaced with running Spark Executors on Ray directly.
* Support save Spark dataset into Ray object store.
* Training mode based on the data preprocessed by Spark.