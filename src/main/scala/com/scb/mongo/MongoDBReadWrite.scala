package com.scb.cduls.sql

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mongodb.spark.config._
import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}
import org.bson.Document

object MongoDBReadWrite {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("read-and-write-from-hdfs")
                     .master("local").appName("MongoSparkConnectorIntro").getOrCreate()
                      
  val readConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/", "database" -> "admin", "collection" -> "log")) 
  val zipDf = spark.read.format("com.mongodb.spark.sql.DefaultSource")
                    .option("partitioner", "spark.mongodb.input.partitionerOptions.MongoPaginateBySizePartitioner")
                    .mongo(readConfig)
  val customRdd = MongoSpark.load(spark, readConfig)
  println(customRdd.count())
  customRdd.printSchema()
  
  }
}