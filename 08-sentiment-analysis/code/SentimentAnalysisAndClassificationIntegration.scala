// Databricks notebook source
// DBTITLE 1,To unmount units as required
dbutils.fs.unmount("/mnt/output")
dbutils.fs.unmount("/mnt/input")
dbutils.fs.unmount("/mnt/capture")

// COMMAND ----------

// DBTITLE 1,Map Data Lake storage as units in the cluster
val configs = scala.collection.immutable.Map (
  dbutils.secrets.get(scope = "stdlintegrationdev-secret-scope", key = "stdlintegrationdev-path") ->
  dbutils.secrets.get(scope = "stdlintegrationdev-secret-scope", key = "stdlintegrationdev")
)

// Mount units
var outputPath = "/mnt/output"
var inputPath = "/mnt/input"
var capturePath = "/mnt/capture"

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(outputPath))
  dbutils.fs.mount(
      source = "wasbs://dbw-output@stdlintegrationdev.blob.core.windows.net/",
      mountPoint = outputPath,
      extraConfigs = configs
  )

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(inputPath))
  dbutils.fs.mount(
      source = "wasbs://dbw-input@stdlintegrationdev.blob.core.windows.net/",
      mountPoint = inputPath,
      extraConfigs = configs
  )

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(capturePath))
  dbutils.fs.mount(
      source = "wasbs://eh-capture@stdlintegrationdev.blob.core.windows.net/",
      mountPoint = capturePath,
      extraConfigs = configs
  )

// COMMAND ----------

// DBTITLE 1,Loading the Football and Non-Football keywords dataset
val footballDf = spark.read
             .option("header", true)
             .csv("/mnt/input/keywords-football.csv")
val nonFootballDf = spark.read
             .option("header", true)
             .csv("/mnt/input/keywords-nonfootball.csv")

val fDict = footballDf.select($"f").filter($"f".isNotNull).collect.map(_.toSeq).flatten.map(_.asInstanceOf[String])
val nfDict = nonFootballDf.select($"nf").filter($"nf".isNotNull).collect.map(_.toSeq).flatten.map(_.asInstanceOf[String])

// COMMAND ----------

// DBTITLE 1,UDF to classify a text in Football/Non-Football
implicit def bool2int(b:Boolean) = if (b) 1 else 0
// User Defined Function for processing content of messages to return their category.
val whichCategory =
    udf((tw: String) =>
        {
            var cat = 0
          
            var twLower = tw.toLowerCase()
            val twResult = twLower.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')     


            for (el <- fDict) {
              cat = cat + twResult.contains(el)
            }
            for (el <- nfDict) {
              cat = cat - twResult.contains(el)
            }

            if (cat > 0) {
              "Football"
            }
            else {
              "Non Football"
            }
        }
    )

// COMMAND ----------

// DBTITLE 1,To test the text classification
val testDF = Seq(
  ("IRPH: la banca celebra la última decisión de la Audiencia de Barcelona https://t.co/lUbzXYTIKM via @diario_16"),
  ("¿Y del Betis eres?"),
  ("Danilo es peor que Pablo Escobar le roba a los más vulnerables de la sociedad sin ningún tipo de escrúpulo ni piedad"),
  ("Hoy es un día grande para el @CDRinconOficial, que recibe en el partido más importante de su historia al @Alaves en la Copa del Rey. https://t.co/cQ2UoHbDjm")
).toDF("text")

val result = testDF.withColumn("category", whichCategory($"text"))

display(result)

// COMMAND ----------

// DBTITLE 1,Object to use Azure Cognitive Services
import java.io._
import java.net._
import java.util._
import javax.net.ssl.HttpsURLConnection
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import scala.util.parsing.json._

case class Language(documents: Array[LanguageDocuments], errors: Array[Any]) extends Serializable
case class LanguageDocuments(id: String, detectedLanguages: Array[DetectedLanguages]) extends Serializable
case class DetectedLanguages(name: String, iso6391Name: String, confidenceScore: Double) extends Serializable

case class Sentiment(documents: Array[SentimentDocuments], errors: Array[Any]) extends Serializable
case class SentimentDocuments(id: String, score: Double) extends Serializable

case class RequestToTextApi(documents: Array[RequestToTextApiDocument]) extends Serializable
case class RequestToTextApiDocument(id: String, text: String, var language: String = "") extends Serializable

//_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-

object DetectorObj extends Serializable {

    // Cognitive Services API connection settings
    val accessKey = "ACCESS_KEY"
    val host = "https://ggcognitiveservices.cognitiveservices.azure.com"
    val languagesPath = "/text/analytics/v3.0/languages"
    val sentimentPath = "/text/analytics/v3.0/sentiment"
    val languagesUrl = new URL(host+languagesPath)
    val sentimenUrl = new URL(host+sentimentPath)
    val g = new Gson

    def getConnection(path: URL): HttpsURLConnection = {
        val connection = path.openConnection().asInstanceOf[HttpsURLConnection]
        connection.setRequestMethod("POST")
        connection.setRequestProperty("Content-Type", "application/json")
        connection.setRequestProperty("Accept", "application/json")      
        connection.setRequestProperty("Ocp-Apim-Subscription-Key", accessKey)
        connection.setDoOutput(true)
        return connection
    }

    def prettify (json_text: String): String = {
        val parser = new JsonParser()
        val json = parser.parse(json_text).getAsJsonObject()
        val gson = new GsonBuilder().setPrettyPrinting().create()
        return gson.toJson(json)
    }

    // Handles the call to Cognitive Services API.
    def processUsingApi(request: RequestToTextApi, path: URL): String = {
        val requestToJson = g.toJson(request)
        val encoded_text = requestToJson.getBytes("UTF-8")
        val connection = getConnection(path)
        val wr = new DataOutputStream(connection.getOutputStream())
        wr.write(encoded_text, 0, encoded_text.length)
        wr.flush()
        wr.close()

        val response = new StringBuilder()
        val in = new BufferedReader(new InputStreamReader(connection.getInputStream()))
        var line = in.readLine()
        while (line != null) {
            response.append(line)
            line = in.readLine()
        }
        in.close()
        return response.toString()
    }

    // Calls the language API for specified documents.
    def getLanguage (inputDocs: RequestToTextApi): String = {
          try {
            val response = processUsingApi(inputDocs, languagesUrl)
            // In case we need to log the json response somewhere
            val niceResponse = prettify(response)
            
            val parser = new JsonParser()
            val json = parser.parse(niceResponse).getAsJsonObject()
            
            val isoName = json.getAsJsonArray("documents").get(0).getAsJsonObject().get("detectedLanguage").getAsJsonObject().get("iso6391Name").getAsString()

            if (isoName == "(Unknown)")
                return "None"

            return isoName
          } catch {
              case e: Exception => return "none"
          }
    }

    // Calls the sentiment API for specified documents. Needs a language field to be set for each of them.
    def getSentiment (inputDocs: RequestToTextApi): String = {
        try {
            val response = processUsingApi(inputDocs, sentimenUrl)
            val niceResponse = prettify(response)

            try {
              val parser = new JsonParser()
              val json = parser.parse(niceResponse).getAsJsonObject()
              val sentimentVal = json.getAsJsonArray("documents").get(0).getAsJsonObject().get("sentiment").getAsString()
              println("Returning sentiment: " + sentimentVal)
              return sentimentVal
            } catch {
                case e: Exception => return "none"
            }
        } catch {
            case e: Exception => return "none"
        }
    }
}

// COMMAND ----------

// DBTITLE 1,UDF to get language of a text using Azure Cognitive Services
// User Defined Function for processing content of messages to return their sentiment.
val whichLanguage =
    udf((textContent: String) =>
        {
            val inputObject = new RequestToTextApi(Array(new RequestToTextApiDocument(textContent, textContent)))
            val detectedLanguage = DetectorObj.getLanguage(inputObject)
          
            if (detectedLanguage == "none") {
                "none"
            } else {
                detectedLanguage
            }
        }
    )

// COMMAND ----------

// DBTITLE 1,Main stream analysis
// Import Spark NLP
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.annotators.ner.NerConverter
import com.johnsnowlabs.nlp.base._
import org.apache.spark.ml.Pipeline

import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

import java.util.zip.GZIPInputStream

// Build connection string with the provided information
val namespaceName = "NAMESPACE_NAME"
val eventHubName = "EVENT_HUB_NAME"
val sasKeyName = "SAS_KEY_NAME"
val sasKey = "SAS_KEY"
val connectionString = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val eventHubsConf = EventHubsConf(connectionString.toString())
  .setStartingPosition(org.apache.spark.eventhubs.EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(100)

// Destination connection
val namespaceName2 = "NAMESPACE_NAME_2"
val eventHubName2 = "EVENT_HUB_NAME_2"
val sasKeyName2 = "SAS_KEY_NAME_2"
val sasKey2 = "SAS_KEY_2"
val connStrs = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
            .setNamespaceName(namespaceName2)
            .setEventHubName(eventHubName2)
            .setSasKeyName(sasKeyName2)
            .setSasKey(sasKey2)

val ehWriteConf = EventHubsConf(connStrs.toString())

var incomingStream = 
  spark.readStream
    .format("eventhubs")
    .options(eventHubsConf.toMap)
    .load()

val decompress = udf{compressed: Array[Byte] => {
  val inputStream = new GZIPInputStream(new ByteArrayInputStream(compressed))
  scala.io.Source.fromInputStream(inputStream).mkString
}}

//val sentimentPipeline = PretrainedPipeline("analyze_sentimentdl_use_twitter", "en")
val sentimentPipeline = PretrainedPipeline("analyze_sentiment", "en")

val tweetSchema = new StructType()
  .add("full_tweet", StringType)
  .add("event_id", StringType)
  .add("full_text", StringType)
  .add("retweet_count", StringType)
  .add("favorite_count", StringType)

val preProcessedData = incomingStream
  .withColumn("data", from_json(decompress($"body"), ArrayType(tweetSchema)))
  .withColumn("data", explode($"data"))
  .withColumn("idEvent", $"data.event_id")
  .withColumn("tweet_text", $"data.full_text")
  .withColumn("full_tweet", $"data.full_tweet")

val languageData = preProcessedData
  .withColumn("language", whichLanguage($"tweet_text"))
  .withColumn("category", whichCategory($"tweet_text"))

val preSentimentAnalysisData = languageData
  .withColumn("positive", lit(0))
  .withColumn("neutral", lit(0))
  .withColumn("negative", lit(0))
  .select("idEvent", "data.full_text", "data.retweet_count", "data.favorite_count", "positive", "neutral", "negative", "language", "category", "data.full_tweet")

val sentimentOutput = sentimentPipeline.annotate(preSentimentAnalysisData.filter($"language" === "en").filter($"category" === "Football"), "full_text")
  .withColumnRenamed("sentiment", "sentiment_result")
  .withColumn("sentiment", element_at($"sentiment_result.result", 1))
  .withColumn("positive", when($"sentiment" === "positive", 1).otherwise(0))
  .withColumn("neutral", when($"sentiment" === "neutral", 1).otherwise(0))
  .withColumn("negative", when($"sentiment" === "negative", 1).otherwise(0))
  .withColumn("full_text", $"text")
  .select("idEvent", "full_text", "retweet_count", "favorite_count", "positive", "neutral", "negative", "language", "category", "full_tweet")
  .union(preSentimentAnalysisData.filter($"language" !== "en"))
  .union(preSentimentAnalysisData.filter($"language" === "en").filter($"category" !== "Football"))

/*val sentimentOutput = languageData
  .select("idEvent", "data.full_text", "data.retweet_count", "data.favorite_count", "language_result")*/

sentimentOutput.printSchema

// This commented code is usefull to debug printing the stream in the console
/*val writer = sentimentOutput
  .writeStream
  .outputMode("append")
  .format("console")
  .option("truncate", false)
  .start()
  .awaitTermination()*/

sentimentOutput
  .toJSON
  .toDF("body")
  .writeStream
  .format("eventhubs")
  .options(ehWriteConf.toMap)
  .option("checkpointLocation", "/mnt/output/checkpoint")
  .start()
  .awaitTermination()
