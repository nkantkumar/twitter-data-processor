package com.scb.tweet

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils

object TwittterDataProcessor {
  def main(args: Array[String]) {

    val consumerKey = "5BVVgmNnwhlXBBYkIlSoYNT4j";
    val consumerSecret = "DVQcg1x67r5D1KP4RvOMlAyCIS4hdRy0KT67Yn0RCAPBJr1fqI";
    val accessToken = "26418719-pMN3nhn4IuZntaaZSFr60Lxc83cGyFBrDsKrHStK0";
    val accessTokenSecret = "HToadt0e7INMkxRSvtGwUYxilVuRXcuQBM1zD7Qlc7TPi";

    val appName = "TwitterDataProcessor"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[3]")

    val ssc = new StreamingContext(conf, Seconds(5))
   
    val filters = args.takeRight(args.length - 4)
    val cb = new ConfigurationBuilder
    cb.setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth))
    val englishTweets = tweets.filter(_.getLang() == "en")
    englishTweets.saveAsTextFiles("hdfs://localhost:9000/user/tweet", "json")
    ssc.start()
    ssc.awaitTermination()

  }
}