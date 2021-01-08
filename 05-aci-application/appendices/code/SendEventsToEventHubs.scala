// Databricks notebook source
// DBTITLE 1,Read keys from Key Vault
// Read secret from Azure Key Vault

val configs = Map (
  dbutils.secrets.get(scope = "<databricks_secret_scope>", key = "<key_name>") ->
  dbutils.secrets.get(scope = "<databricks_secret_scope>", key = "<key_name>")
)

// COMMAND ----------

// DBTITLE 1,Mount Data Lake folders as units in our Cluster
// Mount units

var outputPath = "/mnt/output"
var inputPath = "/mnt/input"

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(outputPath))
  dbutils.fs.mount(
      source = "wasbs://<container_name>@<storage_account_name>.blob.core.windows.net/",
      mountPoint = outputPath,
      extraConfigs = configs
  )

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(inputPath))
  dbutils.fs.mount(
      source = "wasbs://<container_name>@storage_account_name.blob.core.windows.net/",
      mountPoint = inputPath,
      extraConfigs = configs
  )

// COMMAND ----------

// DBTITLE 1,Generate array of keywords
// Generate array of keywords of each event

import scala.collection.mutable.ArrayBuffer

class Keyword(var values: String, var eventId: String) {
 def print = println(s"Keyword values = $values, EventId = $eventId")
}

val keywordsPath = inputPath + "/output/output.parquet"

// Read data from parquet file
var parquetData = spark.read.parquet(keywordsPath)

// Register the DataFrame as a temporary view
parquetData.createOrReplaceTempView("keywords")

// Obtain rows
var parquetRows = spark.sql("SELECT keywords, idEvent FROM keywords")
var parquetRowsCount = parquetRows.count().toInt

var keywordsArray = new ArrayBuffer[Keyword](parquetRowsCount)

parquetRows.collect().foreach { row =>
  var keywords = row.get(0).toString
  var eventId = row.get(1).toString
  var keywordsSplited = keywords.split(",")
  var keywordsSplitedSize = keywordsSplited.size
  var index = 0
  var queryResult = ""
  keywordsSplited.foreach { keyword =>
    var temp = ""
    if (!keyword.isEmpty) {
      if (index < keywordsSplitedSize - 1) {
        temp = "(" + keyword + ") OR "
      } else {
        temp = "(" + keyword + ")"
      }
      queryResult += temp
    }
    index += 1
  }
  var twitterKeywords = new Keyword(queryResult, eventId)
  keywordsArray += twitterKeywords
}

for (keyword <- keywordsArray) {
  keyword.print
}

// COMMAND ----------

// DBTITLE 1,Main block (Calls twitter API)
import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.time.LocalDateTime

  val namespaceName = "<event_hubs_namespace>"
  val eventHubName = "<event_hub_instance>"
  val sasKeyName = "<sas_key_name>"
  val sasKey = dbutils.secrets.get(scope = "<databricks_secret_scope>", key = "<key_name>")
  val connStr = new ConnectionStringBuilder()
              .setNamespaceName(namespaceName)
              .setEventHubName(eventHubName)
              .setSasKeyName(sasKeyName)
              .setSasKey(sasKey)

  val pool = Executors.newScheduledThreadPool(1)
  val eventHubClient = EventHubClient.create(connStr.toString(), pool)

  def sleep(time: Long): Unit = Thread.sleep(time)

  def sendEvent(message: String, eventId: String, delay: Long) = {
    sleep(delay)
    val messageData = EventData.create(message.getBytes("UTF-8"))
    messageData.getProperties().put("adfEventId", eventId)
    eventHubClient.get().send(messageData)
    println("Event Sent at: " + LocalDateTime.now())
  }

  import twitter4j._
  import twitter4j.TwitterFactory
  import twitter4j.Twitter
  import twitter4j.conf.ConfigurationBuilder
  import twitter4j.json.DataObjectFactory

  val twitterConsumerKey = "**REPLACE WITH CONSUMER KEY**"
  val twitterConsumerSecret = "**REPLACE WITH CONSUMER SECRET**"
  val twitterOauthAccessToken = "**REPLACE WITH ACCESS TOKEN**"
  val twitterOauthTokenSecret = "**REPLACE WITH TOKEN SECRET**"

  val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
    .setJSONStoreEnabled(true)
    .setOAuthConsumerKey(twitterConsumerKey)
    .setOAuthConsumerSecret(twitterConsumerSecret)
    .setOAuthAccessToken(twitterOauthAccessToken)
    .setOAuthAccessTokenSecret(twitterOauthTokenSecret)

  val twitterFactory = new TwitterFactory(cb.build())
  val twitter = twitterFactory.getInstance()
  val queryCount = 50
  val threadSleepTime = 500

  while (true) {
    for (keyword <- keywordsArray) {
      val query = new Query(keyword.values)
      println("Tweets processed for: " + keyword.values)
      query.setCount(queryCount)
      query.lang("en")
      val result = twitter.search(query)
      val statuses = result.getTweets()
      var lowestStatusId = Long.MaxValue
      for (status <- statuses.asScala) {
        val tweetString = DataObjectFactory.getRawJSON(status)
        sendEvent(tweetString, keyword.eventId, threadSleepTime)
        lowestStatusId = Math.min(status.getId(), lowestStatusId)
      }
      query.setMaxId(lowestStatusId - 1)
    }
  }

  // Closing connection to the Event Hub
  eventHubClient.get().close()
