import com.johnsnowlabs.nlp.SparkNLP
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import org.apache.spark.ml.Pipeline

// Load Datasets

val trainDataset = spark.read
  .option("header", true)
  .csv("mnt/input/football-covid_2K.csv")

val testDataset = spark.read
  .option("header", true)
  .csv("mnt/input/football-covid_600_en.csv")

// Train Classifier

SparkNLP.version

val documentAssembler = new DocumentAssembler()
   .setInputCol("description")
   .setOutputCol("document")

val token = new Tokenizer()
  .setInputCols("document")
  .setOutputCol("token")

val embeddings = WordEmbeddingsModel.pretrained("glove_100d", lang = "en")
  .setInputCols("document", "token")
  .setOutputCol("embeddings")
  .setCaseSensitive(false)

//convert word embeddings to sentence embeddings
val sentenceEmbeddings = new SentenceEmbeddings()
  .setInputCols("document", "embeddings")
  .setOutputCol("sentence_embeddings")
  .setStorageRef("glove_100d")

//ClassifierDL accepts SENTENCE_EMBEDDINGS 
//UniversalSentenceEncoder or SentenceEmbeddings can produce SENTECE_EMBEDDINGS
val docClassifier = new ClassifierDLApproach()
  .setInputCols("sentence_embeddings")
  .setOutputCol("class")
  .setLabelColumn("category")
  .setBatchSize(64)
  .setMaxEpochs(5)
  .setLr(5e-3f)
  .setDropout(0.5f)

val pipeline = new Pipeline()
  .setStages(
    Array(
      documentAssembler,
      token,
      embeddings,
      sentenceEmbeddings,
      docClassifier
    )
  )

// Let's train our multi-class classifier
val pipelineModel = pipeline.fit(trainDataset)

// Test Classifier

val hardcodedDF = spark.createDataFrame(Seq(
  (1, "#Coronavirus update (13th Aug)lIndia's #COVID19 tally rises to 23,96,638 including 6,53,622 active cases, 16,95,98Ÿ?? https://t.co/92DbEYyo7w"),
  (2, "@Football__Tweet: Football is cruel. #WorldCup #DEN https://t.co/rYBBsrm0JE")
)).toDF("id", "description")

val prediction = pipelineModel.transform(hardcodedDF).select(element_at($"class.result", 1))

display(prediction)

// Test Classifier in Streaming Mode

import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import java.util.zip.GZIPInputStream
import java.io.ByteArrayInputStream
import org.apache.spark.sql.functions._

// Add here the corresponding configuration for the incoming Event Hub
val connectionString = org.apache.spark.eventhubs.ConnectionStringBuilder("CONNECTION-STRING")
  .setEventHubName("EVENT-HUB-NAME")
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(org.apache.spark.eventhubs.EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(100)

var incomingStream = 
  spark.readStream
    .format("eventhubs")
    .options(eventHubsConf.toMap)
    .option("rowsPerSecond",10)
    .load()

val decompress = udf{compressed: Array[Byte] => {
  val inputStream = new GZIPInputStream(new ByteArrayInputStream(compressed))
  scala.io.Source.fromInputStream(inputStream).mkString
}}

val tweetSchema = new StructType()
  .add("event_id", StringType)
  .add("full_text", StringType)
  .add("retweet_count", StringType)
  .add("favorite_count", StringType)

val preProcessedData = incomingStream
  .withColumn("data", from_json(decompress($"body"), tweetSchema))
  .withColumn("idEvent", $"data.event_id")
  .withColumn("description", $"data.full_text")
  .withColumn("id",$"data.event_id")
  .select("id", "description")

val prediction = pipelineModel.transform(preProcessedData)

val writer = prediction
  .writeStream
  .outputMode("append")
  .format("console")
  .option("truncate", false)
  .start()
  .awaitTermination()
