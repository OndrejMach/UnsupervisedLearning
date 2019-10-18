# Unsupervised learning in Spark (K-Means)

This project implements K-Means algorithm for clustering of usage data. Probable use case is subscriber segmentation based on certain values.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.
### Configuration

Configuration JSON file can be found in __src/main/resources/application.conf__. There is basically just a couple of parameters to set:
* **sparkAppName** and **sparkMaster** for setting up the application on the cluster or in the local mode.
* **inputDataLocation**, **rawSummaryFile**, **clusterStatsFile**, **crossDimensionalStatsFile** and **outputFile** for setting paths where files are either read from or written to.
* **writeMode** to specify whether results shall be stored as excel sheets (value 'excel') or as parquet files ('parquet')

parameters can be set in the mentioned file, or via environment variables (for example settings.sparkAppName = 'blabla') or as JVM parameters when executed.

### Prerequisites

First thing you need to do is to build a Jar file which is submitted to a cluster. 
Building a fat jar with all the dependencies included can be done as follows:
```
gradle shadow shadowJar
```
Jar file is then available in __build/libs/__
### Deployment

On a yarn cluster application can be executed as follows:
```
spark-submit \
  --class com.openbean.bd.unsupervisedlearning.Application \
  --master yarn \
  --deploy-mode client \
  --conf <key>=<value> \
  ... # other options \
   UnsupervisedLearning-1.0-SNAPSHOT-all.jar
```

## Authors

* **Ondrej Machacek** - *Implementationk*
* **Timur Karabibier** - **Analysis**
