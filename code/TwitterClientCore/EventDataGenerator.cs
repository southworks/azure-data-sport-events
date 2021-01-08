//********************************************************* 
// 
//    Copyright (c) Microsoft. All rights reserved. 
//    This code is licensed under the Microsoft Public License. 
//    THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF 
//    ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING ANY 
//    IMPLIED WARRANTIES OF FITNESS FOR A PARTICULAR 
//    PURPOSE, MERCHANTABILITY, OR NON-INFRINGEMENT. 
// 
//*********************************************************

using System;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using Microsoft.Azure.EventHubs;
using System.Collections.Generic;
using AdfOutputReader.KeywordObject;
using Newtonsoft.Json;
using System.Text;

namespace TwitterClient
{
    public class StreamingData
    {
        public string full_tweet;
        public string event_id;
        public string full_text;
        public long retweet_count;
        public long favorite_count;
    }
    internal class EventDataGenerator : IObserver<string>
    {
        private readonly int maxSizePerMessageInBytes;
        private readonly IObserver<EventData> eventDataOutputObserver;
        private readonly Stopwatch waitIntervalStopWatch = Stopwatch.StartNew();
        private readonly TimeSpan maxTimeToBuffer;

        private MemoryStream memoryStream;
        private GZipStream gzipStream;
        private StreamWriter streamWriter;

        private long messagesCount = 0;
        private long tweetCount = 0;

        private List<EventKeyword> keywordList;

        public EventDataGenerator(IObserver<EventData> eventDataOutputObserver, int maxSizePerMessageInBytes, int maxSecondsToBuffer, List<EventKeyword> keywordList)
        {
            this.maxTimeToBuffer = TimeSpan.FromSeconds(maxSecondsToBuffer);
            this.maxSizePerMessageInBytes = maxSizePerMessageInBytes;
            this.eventDataOutputObserver = eventDataOutputObserver;
            this.memoryStream = new MemoryStream(this.maxSizePerMessageInBytes);
            this.gzipStream = new GZipStream(this.memoryStream, CompressionMode.Compress);
            this.streamWriter = new StreamWriter(this.gzipStream);
            this.keywordList = keywordList;
        }

        public void OnCompleted()
        {
            this.SendEventData(isCompleted: true);
            Console.WriteLine($"Completed Sent TweetCount = {this.tweetCount} MessageCount = {this.messagesCount}");
            this.eventDataOutputObserver.OnCompleted();
        }

        public void OnError(Exception error)
        {
            this.eventDataOutputObserver.OnError(error);
        }

        public void OnNext(string value)
        {
            dynamic jsonData = JsonConvert.DeserializeObject(value);
            string fullTweetText = jsonData.ToString();
            string tweetText = getTweetText(jsonData);
            List<string> adfEventIds = GetAdfEventIds(jsonData);

            List<StreamingData> tweets = new List<StreamingData>();

            foreach (string adfEventId in adfEventIds) {
                StreamingData streamingData = new StreamingData();
                streamingData.full_tweet = fullTweetText;
                streamingData.event_id = adfEventId;
                streamingData.full_text = tweetText;
                streamingData.retweet_count = jsonData.retweeted_status == null ? jsonData.retweet_count : jsonData.retweeted_status.retweet_count;
                streamingData.favorite_count = jsonData.retweeted_status == null ? jsonData.favorite_count : jsonData.retweeted_status.favorite_count;

                tweets.Add(streamingData);

                this.tweetCount++;
            }

            value = JsonConvert.SerializeObject(tweets);

            Console.WriteLine("Tweet(s) processed: " + value);

            this.streamWriter.WriteLine(value);
            
            this.streamWriter.Flush();
            if (this.waitIntervalStopWatch.Elapsed > this.maxTimeToBuffer || this.memoryStream.Length >= this.maxSizePerMessageInBytes)
            {
                this.SendEventData();
            }
        }

        public string getTweetText(dynamic jsonData) {
            string result = jsonData.text;
            string truncatedField = jsonData.truncated == null ? "false" : jsonData.truncated;
            bool truncated = bool.Parse(truncatedField);
            string textField = jsonData.text;
            bool retweet = textField.Length > 1 && textField.Substring(0,2) == "RT";

            if (retweet && !(jsonData.retweeted_status == null)) {
                result = jsonData.retweeted_status.extended_tweet == null ? jsonData.retweeted_status.text : jsonData.retweeted_status.extended_tweet.full_text;
            } else {
                if (truncated && !retweet) {
                    result = jsonData.extended_tweet.full_text;
                }
            }

            return result;
        }

        public List<string> GetAdfEventIds(dynamic jsonData) {
            string jsonDataParsed = Convert.ToString(jsonData);
            List<string> eventIds = new List<string>();
            foreach (EventKeyword keyword in keywordList) {
                string values = keyword.Values;
                string[] valuesSplited = values.Split(",");
                foreach (string s in valuesSplited) {
                    if (jsonDataParsed.IndexOf(s, StringComparison.OrdinalIgnoreCase) >= 0) {
                        eventIds.Add(keyword.EventId);
                        break;
                    }
                }
            }
            return eventIds;
        }

        private void SendEventData(bool isCompleted = false)
        {
            if (this.memoryStream.Length == 0)
            {
                return;
            }

            this.messagesCount++;
            this.gzipStream.Close();
            var eventData = new EventData(this.memoryStream.ToArray());
            this.eventDataOutputObserver.OnNext(eventData);

            this.gzipStream.Dispose();
            this.memoryStream.Dispose();
            if (!isCompleted)
            {
                this.memoryStream = new MemoryStream(this.maxSizePerMessageInBytes);
                this.gzipStream = new GZipStream(this.memoryStream, CompressionMode.Compress);
                this.streamWriter = new StreamWriter(this.gzipStream);
            }

            Console.WriteLine($"Time: {DateTime.UtcNow:o} Sent TweetCount = {this.tweetCount} MessageCount = {this.messagesCount}");
            this.waitIntervalStopWatch.Restart();
        }
    }
}