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
using System.Linq;
using System.Configuration;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;

using Azure.Storage.Files.DataLake;
using System.Collections.Generic;
using AdfOutputReader.models;
using AdfOutputReader;
using AdfOutputReader.KeywordObject;
using Helpers.AzureDataLake;
using Helpers.KeyVault;
using System.IO;

namespace TwitterClient
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Create Azure Key Vault Helper
            AzureKeyVaultHelper azureKeyVaultHelper = new AzureKeyVaultHelper();

            // Set variables
            string keyVaultName = ConfigurationManager.AppSettings["KeyVaultName"];
            string stAccountName = ConfigurationManager.AppSettings["StAccountName"];
            string stAccountSecretName = ConfigurationManager.AppSettings["StAccountSecretName"];
            string stAccountKey = azureKeyVaultHelper.GetSecret(stAccountSecretName, keyVaultName);
            string stAccountContainer = ConfigurationManager.AppSettings["StAccountContainer"];
            string stAccountDirectory = ConfigurationManager.AppSettings["StAccountDirectory"];
            string keywordFilename = ConfigurationManager.AppSettings["KeywordFilename"];
            string synonymsFilename = ConfigurationManager.AppSettings["TeamsSynonyms"];
            string keywordFilePath = Path.GetTempFileName();
            string synonymsFilePath = Path.GetTempFileName();
            string ehConnectionStringSecretName = ConfigurationManager.AppSettings["EhConnectionString"];
            string ehConnectionStringSecretValue = azureKeyVaultHelper.GetSecret(ehConnectionStringSecretName, keyVaultName);

            // Create Data Lake Service Client
            AzureDataLakeHelper azureDataLakeHelper = new AzureDataLakeHelper(stAccountDirectory, stAccountName, stAccountKey);
            DataLakeServiceClient dataLakeServiceClient = azureDataLakeHelper.GetDataLakeServiceClient();

            // Get file system
            DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(stAccountContainer);

            // Download CSVs file
            await azureDataLakeHelper.DownloadFile(dataLakeFileSystemClient, keywordFilename, keywordFilePath);
            await azureDataLakeHelper.DownloadFile(dataLakeFileSystemClient, synonymsFilename, synonymsFilePath);

            // Create adf helper
            AdfOutputReaderHelper adfOutputReaderHelper = new AdfOutputReaderHelper(keywordFilePath, synonymsFilePath);

            // Read Synonyms file
            List<TeamSynonym> synonymsList = adfOutputReaderHelper.ReadSynonymsFile();

            // Read Keyword's file (used by EventDataGenerator)
            List<EventKeyword> keywordList = adfOutputReaderHelper.ReadAdfOutputFile(synonymsList);

            // Create string with all the kewyords (used by TwitterStream)
            string allKeywords = String.Empty;
            foreach (EventKeyword keyword in keywordList) {
                allKeywords = allKeywords == String.Empty ? keyword.Values : allKeywords + "," + keyword.Values;
            }

            Console.WriteLine("Keywords readed: " + allKeywords);

            //Configure Twitter OAuth
            var oauthCustomerKeyName = ConfigurationManager.AppSettings["OauthConsumerKey"];
            var oauthConsumerSecretName = ConfigurationManager.AppSettings["OauthConsumerSecret"];
            var oauthTokenName = ConfigurationManager.AppSettings["OauthToken"];
            var oauthTokenSecretName = ConfigurationManager.AppSettings["OauthTokenSecret"];
            var oauthCustomerKeyValue = azureKeyVaultHelper.GetSecret(oauthCustomerKeyName, keyVaultName);
            var oauthConsumerSecretValue = azureKeyVaultHelper.GetSecret(oauthConsumerSecretName, keyVaultName);
            var oauthTokenValue = azureKeyVaultHelper.GetSecret(oauthTokenName, keyVaultName);
            var oauthTokenSecretValue = azureKeyVaultHelper.GetSecret(oauthTokenSecretName, keyVaultName);
            string keywords = allKeywords;

            //Configure EventHub
            var ehConnectionBuilder = new EventHubsConnectionStringBuilder(ehConnectionStringSecretValue)
            {
                EntityPath = ConfigurationManager.AppSettings["EventHubName"]
            };

            EventHubClient client = EventHubClient.CreateFromConnectionString(ehConnectionBuilder.ToString());
            Console.WriteLine($"Sending data eventhub : {client.EventHubName} PartitionCount = {client.GetRuntimeInformationAsync().Result.PartitionCount}");
            
            IObservable<string> twitterStream = TwitterStream.StreamStatuses(
                new TwitterConfig(
                    oauthTokenValue,
                    oauthTokenSecretValue,
                    oauthCustomerKeyValue, 
                    oauthConsumerSecretValue,
                    keywords))
                    .ToObservable();

            //int maxMessageSizeInBytes = 250 * 1024;
            //int maxSecondsToBuffer = 20;

            int maxMessageSizeInBytes = 1 * 1024;
            int maxSecondsToBuffer = 1;

            IObservable<EventData> eventDataObserver = Observable.Create<EventData>(
                outputObserver => twitterStream.Subscribe(
                    new EventDataGenerator(outputObserver, maxMessageSizeInBytes, maxSecondsToBuffer, keywordList)));
            
            // keep upto 5 ongoing requests.
            int maxRequestsInProgress = 5;
            IObservable<Task> sendTasks = eventDataObserver
            .Select(e => client.SendAsync(e))
            .Buffer(TimeSpan.FromMinutes(1), maxRequestsInProgress)
            .Select(sendTaskList => Task.WhenAll(sendTaskList));
            
            var subscription = sendTasks.Subscribe(
                sendEventDatasTask => sendEventDatasTask.Wait(),
                e => Console.WriteLine(e));
        }
    }
}