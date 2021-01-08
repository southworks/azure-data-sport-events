# Appendix B: Tweets processed multiple times

In this appendix we will see the first approach to avoid process a tweet multiple times. The complete code can be found [here](./code/SendEventsToEventHubs-v2.0.scala).

When we make a call to the Twitter API, we define how many tweets we want by keyword, but there is no way to prevent them from being repeated in different executions. The initial solution we proposed was to store the event IDs of each tweet in a parquet file, in this way when processing a new tweet it can be reviewed in that file to see if it has already been processed. Let's see the new cells that we added and what functions they fulfill:

1. First, we defined a new constant in a cell, this contains the path to find the **tweetdId** file.

    ```scala
    val tweetIdPath = outputPath + "/tweetsId"
    ```

2. Create Tweets event Id File: This cell will generate an empty **parquet file**, and this cell should be executed each time we want to delete all the **event Id's** stored.

    ```scala
    val df = Seq.empty[String].toDF() //an empty dataframe created
    df.write.mode(SaveMode.Overwrite).parquet(tweetIdPath)
    ```

3. Get rows of Tweets event Id file: This cell will retrieve all the rows of the **parquet file**.

    ```scala
    var tweetsIdProcessed = spark.read.parquet(tweetIdPath)
    tweetsIdProcessed.createOrReplaceTempView("tweetsId")
    var parquetRows = spark.sql("SELECT * FROM tweetsId")
    var parquetRowsCount = parquetRows.count().toInt
    ```

4. Support functions: This cells contains functions used in the main cells before sending the events to _Event Hub_.

    ```scala
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

    def updateParquetFile (df: DataFrame) {
      df.write.mode(SaveMode.Overwrite).parquet(tweetIdPath)
    }

    def addEventIdToParquetFile (eventId: String) {
      var df = Seq(eventId).toDF()
      tweetsIdProcessed = tweetsIdProcessed.union(df)
      updateParquetFile(tweetsIdProcessed)
    }
    ```

5. The function **initTweetsIdFile** reads the updated **tweetId** file after an insertion occurs.

>💡 **Hint**:
>
>  It is possible the underlying files have been updated. You can explicitly invalidate the cache in Spark by running 'REFRESH TABLE tableName' command in SQL or by recreating the Dataset/DataFrame involved.

6. The function **existEventId** searches if the current **event id** read was processed.
7. The function **updateParquetFile** writes in the parquet file the dataframe updated.
8. The function **addEventIdToParquetFile** merges the current dataframe with the incoming event, and then invokes the function **updateParquetFile**.
9. Finally, the cell with the **main** functionality, in part it is the same as the first solution, there are only changes in this code block.

    ```scala
    import org.json4s.jackson.JsonMethods._ //this import is new
    import org.json4s.DefaultFormats //this import is new

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
    ```

The differences are:

- The lines 15 to 17 are used to get the tweet **event Id**.
- The if statement on line 18 is used to verify if we already processed the current tweet.
- In the line 20 we add the new event Id to the **parquet file**.
- In the line 21 we refresh the file.
- And in the case we already processed the tweet, we show the message on line 23.

This approach works but **is not performant**, first, because the tweet event Id file will contain millions of rows and it will take more time process it each time. Also, reads and writes to files are not performant.
