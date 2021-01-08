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

// DBTITLE 1,A couple of variables
// Create path variables

val keywordsPath = inputPath + "/output/output.parquet"
val tweetIdPath = outputPath + "/tweetsId"

// COMMAND ----------

// DBTITLE 1,Generate array of keywords
// Generate array of keywords of each event

import scala.collection.mutable.ArrayBuffer

class Keyword(var values: String, var eventId: String) {
 def print = println(s"Keyword values = $values, EventId = $eventId")
}

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

// DBTITLE 1,Create Tweets event Id file
// Create an empty parquet file

val df = Seq.empty[String].toDF()
df.write.mode(SaveMode.Overwrite).parquet(tweetIdPath)

// COMMAND ----------

// DBTITLE 1,Get rows of Tweets event Id file
// Retrieve tweet id rows

var tweetsIdProcessed = spark.read.parquet(tweetIdPath)
tweetsIdProcessed.createOrReplaceTempView("tweetsId")
var parquetRows = spark.sql("SELECT * FROM tweetsId")
var parquetRowsCount = parquetRows.count().toInt

// COMMAND ----------

// DBTITLE 1,Support functions
// Define functions

import org.apache.spark.sql.DataFrame
import spark.implicits._

def initTweetsIdFile () {
  tweetsIdProcessed = spark.read.parquet(tweetIdPath)
  tweetsIdProcessed.createOrReplaceTempView("tweetsId")
  parquetRows = spark.sql("SELECT * FROM tweetsId")
  parquetRowsCount = parquetRows.count().toInt
}

def existEventId (currentEventId: String) : Boolean = {
  if (parquetRowsCount > 0) {
    parquetRows.collect().foreach { row =>
      var eventId = row.get(0)
      if (currentEventId == eventId) { 
        return true
      }
    }
  }
  return false
}

def addEventIdToParquetFile (eventId: String) {
  var df = Seq(eventId).toDF()
  tweetsIdProcessed = tweetsIdProcessed.union(df)
  updateParquetFile(tweetsIdProcessed)
}

def updateParquetFile (df: DataFrame) {
  df.write.mode(SaveMode.Overwrite).parquet(tweetIdPath)
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
  import org.json4s.jackson.JsonMethods._
  import org.json4s.DefaultFormats

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
          implicit val formats = DefaultFormats
          val tweetParsed = parse(tweetString)
          val tweetId = (tweetParsed \ "id_str").extract[String]
          if (!existEventId(tweetId)) {
            sendEvent(tweetString, keyword.eventId, threadSleepTime)
            addEventIdToParquetFile(tweetId)
            initTweetsIdFile()
          } else {
            println("The tweet: " + tweetId + " was already been processed")
         }
          lowestStatusId = Math.min(status.getId(), lowestStatusId)
        }
        query.setMaxId(lowestStatusId - 1)
    }
  }

  // Closing connection to the Event Hub
  eventHubClient.get().close()
