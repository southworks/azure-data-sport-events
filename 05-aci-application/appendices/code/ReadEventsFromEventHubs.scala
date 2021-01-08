// Databricks notebook source
import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Build connection string with the above information
val namespaceName = "<event_hubs_namespace>"
val eventHubName = "<event_hub_instance>"
val sasKeyName = "<sas_key_name>"
val sasKey = dbutils.secrets.get(scope = "<databricks_secret_scope>", key = "<key_name>")
val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val customEventhubParameters = EventHubsConf(connStr.toString()).setMaxEventsPerTrigger(100)
val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val tweetSchema = new StructType()
      .add("full_text", StringType)
      .add("retweet_count", LongType)
      .add("favorite_count", LongType)

incomingStream.printSchema

val messages = incomingStream
  .select(explode($"properties"), $"body")
  .withColumn("Body", from_json($"body".cast(StringType), tweetSchema))
  .withColumn("event_id", regexp_replace($"value", "[^A-Z0-9_]", ""))
  .select("event_id", "Body.*")

 val query =
   messages
     .writeStream
     .format("csv")
     .option("header", "true")
     .option("path", "/mnt/output/output")
     .option("checkpointLocation", "/mnt/output/check")
     .start()
