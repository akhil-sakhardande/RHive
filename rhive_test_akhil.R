# This is a test R code to check R-Hive connectivity
# Author: Akhil Sakhardande

# Setting the environment variables
Sys.setenv("HADOOP_HOME"="/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/hadoop/lib")
Sys.setenv("HADOOP_CMD"="/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/hadoop/bin/hadoop")
Sys.setenv("HADOOP_STREAMING"="/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/jars/hadoop-streaming-2.6.0-cdh5.8.0.jar")
Sys.setenv("HIVE_HOME"="/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/hive")
Sys.setenv("JAVA_HOME"="/opt/jdk1.8.0_101")
Sys.setenv("HADOOP_CONF"="/etc/hadoop/conf.cloudera.hdfs")

# Installing the required packages
install.packages("/home/madhav/rJava_0.9-8.tar.gz", repos = NULL, type="source")       
install.packages("/home/madhav/rhdfs_1.0.8.tar.gz", repos = NULL, type="source")
install.packages("/home/madhav/RHive_2.0-0.2.tar.gz", repos = NULL, type="source")
install.packages("/home/madhav/rmr2_3.3.1.tar.gz", repos = NULL, type="source")

# Invoking libraries
library(rJava)
library(rhdfs)
library("RHive")
library(mapReduce)

# Initializing hadoop environment
hdfs.init()
rhive.env(ALL=TRUE)
rhive.init()
rhive.connect(host="hadoop1",port=10000,hiveServer2=TRUE,defaultFS="hdfs://nameservice1/user/madhav")

# Creating a new Iris table into Hive
iris_file <- iris
class(iris_file)
dim(iris_file)
getwd()
write.csv(iris_file, file = "Iris_file.csv", row.names = FALSE)

# This file is used to create a table in Hive environment. This is done from the backend.
# CREATE TABLE Iris_input (Sepal_length string, Sepal_width string, Petal_length string,Petal_width string, Species string) row format delimited fields terminated by ',' stored as textfile;
#  LOAD DATA LOCAL INPATH '/home/madhav/Iris_file.csv' INTO TABLE iris_input;
# select count(*) from iris_input;

# Reading data from Hive in R
rhive.list.tables()
data_file <- rhive.query('select * from iris_input')
class(data_file)
dim(data_file)

# Applying Kmeans to the fetched data
names(data_file) <- c("Sepal.length", "Sepal.width", "Petal.length", "Petal.width", "Species")
data_file <- data_file[-1,]
data_file_kmeans <- kmeans(data_file[,c(1:4)], 3)
head(data_file_kmeans,5)
data_file_kmeans$cluster
data_file1 <- cbind(data_file, data_file_kmeans$cluster)
dim(data_file1)
write.csv(data_file1, file="iris_ouput.csv", row.names = FALSE)

# New table created in Hive for Kmeans results
# CREATE TABLE Iris_output (Sepal_length string, Sepal_width string, Petal_length string, Petal_width string, Species string, Species_Kmeans string) row format delimited fields terminated by ',' stored as textfile;
# LOAD DATA LOCAL INPATH '/home/madhav/iris_ouput.csv' INTO TABLE iris_ouput;LOAD DATA LOCAL INPATH '/home/madhav/iris_ouput.csv' INTO TABLE iris_ouput;
# describe iris_output;
# select count(*) from iris_output;



 




