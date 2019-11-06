# Unsupervised learning in Spark (K-Means)

This project implements K-Means algorithm for clustering of usage data. Probable use case is subscriber segmentation based on certain values.

## Getting Started

These instructions will explain how to build, configure and deploy solution on a cluster. Application is can be executed in two modes:
* **training** input data is used for preparing KMeans models which are stored on a filesystem - as a side product application also stores Summary file of an input data.
* **transformatio** application loads pre-trained models, transforms input data and generates desired output. 

to switch between these two modes commandline parameters are used; 'training' for the training regime and 'transform' for the transformation regime.
Training mode shall be used ad-hoc when you need to re-train KMeans models, transform mode is the one for regularly running pipeline.

### Configuration

Configuration JSON file can be found in __src/main/resources/application.conf__. There is basically just a couple of parameters to set (don't forget **.value** at the end):
* **sparkAppName.value** and **sparkMaster.value** for setting up the application on the cluster or in the local mode.
* **inputDataLocation.value**, **rawSummaryFile.value**, **clusterStatsFile.value**, **crossDimensionalStatsFile.value** and **outputFile.value** for setting paths where files are either read from or written to.
* **writeMode.value** to specify whether results shall be stored as excel sheets (value 'excel') or as parquet files ('parquet')
* **sampleRate** sample ratio of output data to be stored in the output (user_id and assigned cluster IDs)
* **modelPersistence** path where models will be stored - there will be three folders in it for each clustering dimension

parameters can be set in the mentioned file, or via environment variables or as JVM parameters when executed.

when setting configuration parameters from environment variables you need to replace **.** with **_** and change all the letters to **upper case**. 
Example:
```
export SETTINGS_SPARKAPPNAME_VALUE="KMEANS "
```
### Prerequisites

First thing you need to do is to build a Jar file which is submitted to a cluster. 
Building a fat jar with all the dependencies included can be done as follows:
```
gradle shadow shadowJar
```
Jar file is then available in __build/libs/__
### Deployment

On a yarn cluster application can be executed in the **training** mode as follows:
```
spark-submit \
  --class com.openbean.bd.unsupervisedlearning.Application \
  --master yarn \
  --deploy-mode client \
  --conf <key>=<value> \
  ... # other options \
   UnsupervisedLearning-1.0-SNAPSHOT-all.jar training
```

On a yarn cluster application can be executed in the **transformation** mode as follows:
```
spark-submit \
  --class com.openbean.bd.unsupervisedlearning.Application \
  --master yarn \
  --deploy-mode client \
  --conf <key>=<value> \
  ... # other options \
   UnsupervisedLearning-1.0-SNAPSHOT-all.jar training
```

## Authors

* **Ondrej Machacek** - *Implementationk*
* **Timur Karabibier** - **Analysis**
