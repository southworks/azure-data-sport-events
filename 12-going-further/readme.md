# Going Further

The application that has been described in the previous modules works but it has some aspects that could be improved. At this point, we didn't have the time to work on those aspects so we are documenting it here so later it could be continued.

## Tweets Processed Multiple Times

At this point we are not checking if a tweet has already been processed, so a tweet can be read more than once in the case the interface with _Twitter_ is stopped and restarted in a short period of time, that is because the Twitter API returns the most recent tweets every time we perform a search, and if no control of the unique tweet identifier is in place we come with this issue, therefore, the end result will not be 100% realistic. In [this appendix](../05-aci-application/../05-aci-application/appendices/B-tweets-processed.md) we implemented a solution to avoid processing a tweet multiple times when using _Databricks_.

In this initial solution, to avoid processing a tweet more than once what we do is save the **Tweet ID** in a parquet file, then each time we need to process a tweet we look in that file if we already processed it. The current solution saves the **Tweet ID** every time the _Notebook_ process a new tweet, this approach gives us consistency because the _Twitter_ API could get saturated or the _Cluster_ could run out of memory and could lose a _Tweet ID_. The problem with this solution is that the actions of reading and writing take more than 3 seconds each one so that adds the delay and that’s why the tweets were processed slow.

A possible approach could be storing the timestamp of the last execution, so we will accept tweets that are between that timestamp and the current timestamp. With this solution we could avoid reading the same tweet twice, we could avoid reading old tweets and we would avoid having a file with millions of rows.

Here is an approach to obtain the current timestamp and the **created_at** attribute of a tweet in milliseconds in the _Databricks_ written in **Scala**. Then it could be used to compare dates.

```scala
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val tweetDateCreated = "Fri Nov 13 17:53:02 +0000 2020" //format of "created_at" property of a tweet
val tweetDateCreatedSplited = tweetDateCreated.split(" ")
val year = tweetDateCreatedSplited(5)
val month = tweetDateCreatedSplited(1)
val day = tweetDateCreatedSplited(2)
val hour = tweetDateCreatedSplited(3)
val formattedDate = year + "-" + month + "-" + day + " " + hour

val tweetDateCreatedParsed = new java.text.SimpleDateFormat("yyyy-MMM-dd HH:mm:ss").parse(formattedDate)
val tweetDateCreatedParsedTime = tweetDateCreatedParsed.getTime()
val currentTime = System.currentTimeMillis()

println("Current time: " + currentTime) //in milliseconds
println("Tweet created time: " + tweetDateCreatedParsedTime) //in milliseconds
```

This idea was not implemented neither with Databricks nor in the C# application, so it is still a pending aspect for improvement.

## Tweets dates

We also found that some tweets returned by the API seem to be from old dates and thus should not be related to the current events, that in spite that Twitter API is said to return the most recent tweets. Then a filter by date could help to diminish the possibility of processing unrelated tweets.

## Explore other libraries for Sentiment Analysis

Throughout the development we have come across some libraries which, in principle, seemed to be suited to perform the _Sentiment Analysis_ in a tweet. We came up with several alternatives and then chose one. When evaluating the performance we discovered that it is only effective with tweets in _English_. This brought with it two associated problems:

1. The limitation of only being able to process tweets in a single language.
2. How to find out the language in which a tweet is written.

To solve these problems, it was decided to only evaluate the sentiment of tweets written in _English_, so we tried to take advantage of the fact that the library used could also determine the language of a tweet and it was used. Unfortunately, the language analysis was not accurate, so we ended up choosing to use _Cognitive Services_ (which proved highly effective but has a high cost).

This issue leaves open the opportunity to explore some other libraries that might be more effective either to analyze tweets in multiple languages and/or detect language more accurately or continue trying some other approaches to use the NLP library.

## Improvements to Text Classification algorithm

In [this module](../06-text-classification/readme.md), we implemented _Text Classification_ with a manual algorithm, even when the accuracy to classify tweets as Football and Non-Football related is acceptable, it could be further improved by:

 - Improving the dictionaries of Football and Non-Football keywords
 - Further refine the text pre processing to remove stop words, that is neutral words that can be found in any category

## Finalize the Trained Model for text classification

Apart from improving the manual algorithm described above, a Trained Model can be used for _Text Classification_. In [this appendix](../06-text-classification/appendices/A-trained-model.md), we exposed an approach we took to implement _Text Classification_ using a **Trained Model** built on top of the _Spark NLP Library_, however, we mentioned that we also had several issues that prevented us to finish the implementation, the intention of the appendix is to leave all the experience documented and the support files provided so further efforts could be made to try to finish the implementation that can provide more efficiency and accuracy.