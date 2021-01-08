using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Azure.Storage;
using System.IO;
using Azure;
using System;
using System.Threading.Tasks;

namespace Helpers.AzureDataLake
{
    public class AzureDataLakeHelper
    {
        string stAccountDirectory;
        string stAccountName;
        string stAccountKey;

        public AzureDataLakeHelper(string stAccountDirectory, string stAccountName, string stAccountKey) {
            this.stAccountDirectory = stAccountDirectory;
            this.stAccountName = stAccountName;
            this.stAccountKey = stAccountKey;
        }

        public DataLakeServiceClient GetDataLakeServiceClient()
        {
            StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(stAccountName, stAccountKey);
            string dfsUri = "https://" + stAccountName + ".blob.core.windows.net";
            DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient(new Uri(dfsUri), sharedKeyCredential);
            return dataLakeServiceClient;
        }

        public async Task DownloadFile(DataLakeFileSystemClient fileSystemClient, string keywordFilename, string outputFilePath)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(stAccountDirectory);
            DataLakeFileClient fileClient = directoryClient.GetFileClient(keywordFilename);
            Response<FileDownloadInfo> downloadResponse = await fileClient.ReadAsync();
            BinaryReader reader = new BinaryReader(downloadResponse.Value.Content);
            FileStream fileStream = File.OpenWrite(outputFilePath);

            int bufferSize = 4096;
            byte[] buffer = new byte[bufferSize];
            int count;

            while ((count = reader.Read(buffer, 0, buffer.Length)) != 0)
            {
                fileStream.Write(buffer, 0, count);
            }

            await fileStream.FlushAsync();
            fileStream.Close();

        }
    }
}