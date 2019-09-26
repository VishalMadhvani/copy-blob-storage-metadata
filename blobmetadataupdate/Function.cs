using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace blobmetadataupdate
{
    public static class Function
    {
        private static CloudBlobContainer GetBlobContainer()
        {
            //CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageAccount"));
            //CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference("data");
            return cloudBlobContainer;
        }

        private static CloudQueue GetQueue(string queueName)
        {
            //CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageAccount"));
            //CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            CloudQueueClient client = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = client.GetQueueReference(queueName);
            return queue;
        }

        [FunctionName("GetBlobs")]
        public static async Task<HttpResponseMessage> GetBlobs(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            ILogger log)
        {
            CloudBlobContainer blobContainer = GetBlobContainer();
            CloudQueue queue = GetQueue("csvdata");

            //List<string> blobNames = new List<string>();
            BlobContinuationToken blobContinuationToken = null;
            do
            {
                var results = await blobContainer.ListBlobsSegmentedAsync(null, blobContinuationToken);
                // Get the value of the continuation token returned by the listing call.
                blobContinuationToken = results.ContinuationToken;
                //var csvBlobNames = string.Join(',', results.Results.OfType<CloudBlockBlob>().Select(blob => blob.Name));
                //await queue.AddMessageAsync(new CloudQueueMessage(csvBlobNames));
                //blobNames.AddRange(results.Results.OfType<CloudBlockBlob>().Select(blob => blob.Name));
                //var addMessageTasks = blobNames.Select(blobName => queue.AddMessageAsync(new CloudQueueMessage(blobName)));
                //await Task.WhenAll(addMessageTasks);
                foreach (IListBlobItem item in results.Results)
                {
                    //blobNames.Add(((CloudBlob)item).Name);
                    await queue.AddMessageAsync(new CloudQueueMessage(((CloudBlob)item).Name));
                }
            } while (blobContinuationToken != null); // Loop while the continuation token is not null.
            //log.LogWarning($"Got {blobNames.Count} blobs.");

            return req.CreateResponse(System.Net.HttpStatusCode.OK);
        }

        //[FunctionName("SplitCsvBlobs")]
        //public static async Task SplitCsvBlobs(
        //    [QueueTrigger("csvdata", Connection = "StorageAccount")]string csvBlobNames,
        //    ILogger log)
        //{
        //    CloudQueue queue = GetQueue("data");
        //    foreach (var blobName in csvBlobNames.Split(','))
        //    {
        //        await queue.AddMessageAsync(new CloudQueueMessage(blobName));
        //    }
        //}

        [FunctionName("UpdateMetadata")]
        public static async Task UpdateMetadata(
            [QueueTrigger("data", Connection = "StorageAccount")]string blobName,
            ILogger log)
        {
            CloudBlobContainer blobContainer = GetBlobContainer();
            CloudBlockBlob cloudBlockBlob = blobContainer.GetBlockBlobReference(blobName);
            await cloudBlockBlob.FetchAttributesAsync();
            cloudBlockBlob.Metadata.Clear();
            cloudBlockBlob.Metadata.Add("test_key", "test_value");
            await cloudBlockBlob.SetMetadataAsync();
        }
    }
}