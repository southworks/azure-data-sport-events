# Module 5 - ACI Application

# Index 📚
- [C# Application](#c#-application)
  - [Description](#description)
  - [Obtaining the Keywords](#obtaining-the-keywords)
    - [Connect to Data Lake by using an account key](#connect-to-data-lake-by-using-an-account-key)
    - [Reading the Keywords](#reading-the-keywords)
  - [Getting the Tweets](#getting-the-tweets)
    - [Create a Twitter application](#create-a-twitter-application)
    - [Fetching the Tweets](#fetching-the-tweets)
  - [Associating the Tweets to Events](#associating-the-tweets-to-events)
  - [Sending data to Event Hub](#sending-data-to-event-hub)
    - [Connecting to Event Hub](#connecting-to-event-hub)
    - [Sending events to Event Hub](#sending-events-to-event-hub)
  - [Putting all together](#putting-all-together)
- [Deploying in ACI](#deploying-in-aci)
  - [Definitions](#Definitions)
    - [Azure Container Instances](#azure-container-instances)
    - [Azure Container Registry](#azure-container-registry)
  - [Creating the Dockerfile](#creating-the-dockerfile)
  - [Creating an ACR instance](#creating-an-acr-instance)
  - [Uploading and building the project](#uploading-and-building-the-project)
- [Summing up](#summing-up)
- [Next Steps](#next-steps)

# C# Application

## Description

The main objective of this application is to be able to obtain tweets (through the usage of the Twitter API) related to a certain topic and then be processed and sent for further analysis.

The next steps must be followed to perform the processing:

1. Obtain the keywords to use, related to the topic of interest.
2. Make the call to the Twitter API using the fetched keywords.
3. Associate the results to the corresponding topic of interest.
4. Send the results to Event Hub to be analyzed in the next stage.

## Obtaining the Keywords

The keywords are stored in two separate files. On one side, they are obtained according to the upcoming events (the file is updated daily) and, on the other hand, there is a list that contains those that never vary and are always valid.

The files are stored in Data Lake, the first step to obtain them is to connect to the resource.

### Connect to Data Lake by using an account key

This example creates a [DataLakeServiceClient](https://docs.microsoft.com/en-us/dotnet/api/azure.storage.files.datalake.datalakeserviceclient) instance by using an account key.

```csharp
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Azure.Storage;
using System.IO;
using Azure;

public void GetDataLakeServiceClient(ref DataLakeServiceClient dataLakeServiceClient, string accountName, string accountKey) {
    StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);

    string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

    dataLakeServiceClient = new DataLakeServiceClient(new Uri(dfsUri), sharedKeyCredential);
}
```

### Reading the Keywords

To read the keywords, the corresponding files must be obtained by specifying their path and then reading them. First, the files from the Data Lake must be obtained, for that the following method is used:

```csharp
public async Task DownloadFile(DataLakeFileSystemClient fileSystemClient, string keywordFilename, string outputFilePath) {
    DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(stAccountDirectory);
    DataLakeFileClient fileClient = directoryClient.GetFileClient(keywordFilename);
    Response<FileDownloadInfo> downloadResponse = await fileClient.ReadAsync();
    BinaryReader reader = new BinaryReader(downloadResponse.Value.Content);
    FileStream fileStream = File.OpenWrite(outputFilePath);

    int bufferSize = 4096;
    byte[] buffer = new byte[bufferSize];
    int count;

    while ((count = reader.Read(buffer, 0, buffer.Length)) != 0) {
        fileStream.Write(buffer, 0, count);
    }

    await fileStream.FlushAsync();
    fileStream.Close();
}
```

This method creates an empty file locally and then reads the remote file and writes its content to this new file. Then when making the call, the following fields must be provided:

* **fileSystemClient**: Instance with the session started in the desired storage.
* **keywordFilename**: Path of the file from where the Keywords will be read.
* **outputFilePath**: Path of the destination file where the obtained will be written.

Example:

```csharp
await azureDataLakeHelper.DownloadFile(dataLakeFileSystemClient, "output.csv", "temporal.csv");
```

## Getting the Tweets

### Create a Twitter application

If you do not already have a Twitter application that you can use to perform these steps, you can create one. You must already have a Twitter account to proceed with the next steps.

> Note
> 
> The exact process in Twitter for creating an application and getting the keys, secrets, and token might change. If these instructions don't match what you see on the Twitter site, refer to the Twitter developer documentation.

1. From a web browser, go to [Twitter For Developers](https://developer.twitter.com/en/apps), create a developer account, and select **Create an app**. You might see a message saying that you need to apply for a Twitter developer account. After your application has been approved, you should see a confirmation email (it could take several days to be approved for a developer account).

2. In the **Create an application** page, provide the details for the new app, and then select **Create your Twitter application**.

3. In the application page, select the **Keys and Tokens** tab and copy the values for **Consumer API Key** and **Consumer API Secret Key**. Also, select **Create** under **Access Token and Access Token Secret** to generate the access tokens.
    > Note
    > 
    > The keys and secrets for the Twitter application provide access to your Twitter account. Treat this information as sensitive, the same as you do your Twitter password. For example, don't embed this information in an application that you give to others.

### Fetching the Tweets

Now is the time to connect to Twitter and request tweets that match a series of given keywords. For that, the following must be done:

1. Make the connection
	```csharp
	IObservable<string> twitterStream = TwitterStream.StreamStatuses(
                new TwitterConfig(
                    oauthTokenValue,
                    oauthTokenSecretValue,
                    oauthCustomerKeyValue, 
                    oauthConsumerSecretValue,
                    keywords))
                    .ToObservable();
	```
2. Using the connection, request tweets that match a list of keywords that is passed as a parameter:
	```csharp
	IObservable<EventData> eventDataObserver = Observable.Create<EventData>(outputObserver => twitterStream.Subscribe(new EventDataGenerator(outputObserver, maxMessageSizeInBytes, maxSecondsToBuffer, keywordList)));
	```
3. Within the resulting observable, a handler can be created to process each tweet obtained. This will generate a stream of tweets which will be processed as they arrive.

## Associating the Tweets to Events

Each tweet fetched is related to a set of keywords which are explicitly associated with a particular event. This implies that a new object must be assembled that will combine the most relevant data of a tweet with its associated event. To better illustrate the data that will be necessary in this mentioned object, we have the following JSON object with the necessary fields (empty).

```json
{
  "event_id": "",
  "full_tweet": {},
  "full_text": "",
  "retweet_count": 0,
  "favorite_count": 0
}
```

## Sending data to Event Hub

### Connecting to Event Hub

1.  Add the following  `using`  statements to the top of the  **Program.cs**  file:
        
    ```csharp
    using System;
    using System.Text;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Producer;
    ```
    
2.  Add constants to the `Program` class for the Event Hubs connection string and the Event Hub name. Replace placeholders in brackets with the proper values that you got when creating the Event Hub. Make sure that the `{Event Hubs namespace connection string}` is the namespace-level connection string, and not the Event Hub string.
    
    ```csharp
    private const string connectionString = "<EVENT HUBS NAMESPACE - CONNECTION STRING>";
    private const string eventHubName = "<EVENT HUB NAME>";
    
	var ehConnectionBuilder = new EventHubsConnectionStringBuilder(connectionString ) {
                EntityPath = eventHubName
            };

    EventHubClient client = EventHubClient.CreateFromConnectionString(ehConnectionBuilder.ToString());
    ```

### Sending events to Event Hub

To send, starting from an event (`e`), it can be done as follows:

```csharp
client.SendAsync(e);
```

## Putting All Together

The complete application code combining all things shown please can be found [here](../code/).


# Deploying in ACI

## Definitions

### Azure Container Instances

Azure Container Instances (ACI) is a service that allows the creation of application containers without the need to worry about the underlying infrastructure. It offers support for the use of Docker-based containers. For more information you can consult the official [documentation](https://docs.microsoft.com/en-us/azure/container-instances/container-instances-overview).

### Azure Container Registry

Azure Container Registry (ACR) is a service that operates based on Docker which allows the creation of images as well as their hosting to later be used within an instance of a container. For more information you can consult the official [documentation](https://docs.microsoft.com/en-us/azure/container-registry/container-registry-intro).

## Creating the Dockerfile

To compile the application, all the steps must be listed in the Dockerfile as commands, which will then be executed in order. Among the necessary commands is the compilation using .NET Core and the installation of a library within the operating system that acts on the container.

The file will have the following content:

```dockerfile
# https://hub.docker.com/_/microsoft-dotnet
FROM mcr.microsoft.com/dotnet/sdk:3.1 AS build
WORKDIR /source

# copy csproj and restore as distinct layers
COPY *.csproj .
RUN dotnet restore -r linux-musl-x64

# copy and publish app and libraries
COPY . .
RUN dotnet publish -c release -o /app -r linux-musl-x64 --self-contained false --no-restore

# final stage/image
FROM mcr.microsoft.com/dotnet/runtime:3.1-alpine
WORKDIR /app
COPY --from=build /app .

# execution point
ENTRYPOINT ["./TwitterClientCore"]

# additional library installation
RUN apk add --upgrade libsecret-dev
```

## Creating an ACR instance

Create an ACR instance using the [az acr create](https://docs.microsoft.com/en-us/cli/azure/acr#az-acr-create) command. The registry name must be unique within Azure, and contain 5-50 alphanumeric characters.

```bash
az acr create --resource-group <resource_group_name> \ --name <container_registry_name> --sku Basic
```

## Uploading and building the project


In order to compile the code within an ACR instance, three parameters must be taken into account:

1. Have an instance of an ACR created.
2. The name of the target image (which will be created as a result of the execution).
3. The path of the Dockerfile and the code to compile.

The command to execute, taking into account the parameters mentioned above, will be as follows:

```bash
az acr build -r "<acr_instance_name" -f ./Dockerfile -t <image_name>:<image_version> <path_of_root_project>
```

# Summing up

By completing this module we now have an application capable of fetching tweets using the Twitter API, the tweets search is done with a set of keywords related to each existing event. The application can be hosted and run in an ACI instance and uses the Data Lake for storage.

# Next Steps

Go to [Text Classification](../06-text-classification/readme.md)