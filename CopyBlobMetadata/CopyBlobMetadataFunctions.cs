using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace blobmetadataupdate
{
    public static class CopyBlobMetadataFunctions
    {
        [FunctionName("Start")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            CloudQueue queue = GetQueue("getblobs");
            await queue.AddMessageAsync(new CloudQueueMessage("START"));

            return new OkResult();
        }

        [FunctionName("GetBlobs")]
        public static async Task GetBlobs(
            [QueueTrigger("getblobs", Connection = "CopyBlobMetadataQueueConnectionString")]string continuationToken,
            ILogger log)
        {
            if (continuationToken == "START")
                continuationToken = null;

            CloudBlobContainer cloudBlobContainer = GetSourceBlobContainer();
            var results = await cloudBlobContainer.ListBlobsSegmentedAsync(null, true, BlobListingDetails.None, 5000, new BlobContinuationToken() { NextMarker = continuationToken }, null, null);

            continuationToken = results.ContinuationToken?.NextMarker;
            if (continuationToken != null)
            {
                CloudQueue getBlobsQueue = GetQueue("getblobs");
                await getBlobsQueue.AddMessageAsync(new CloudQueueMessage(continuationToken));
            }

            CloudQueue copyMetadataQueue = GetQueue("copymetadata");
            List<string> blobNames = results.Results.OfType<CloudBlockBlob>().Select(blob => blob.Name).ToList();
            foreach (string blobName in blobNames)
            {
                await copyMetadataQueue.AddMessageAsync(new CloudQueueMessage(blobName));
            }
        }

        [FunctionName("CopyMetadata")]
        public static async Task UpdateMetadata(
            [QueueTrigger("copymetadata", Connection = "CopyBlobMetadataQueueConnectionString")]string blobName,
            ILogger log)
        {
            var sourceBlobContainer = GetSourceBlobContainer();
            var sourceBlob = sourceBlobContainer.GetBlockBlobReference(blobName);
            await sourceBlob.FetchAttributesAsync();

            var destinationBlobContainer = GetDestinationBlobContainer();
            var destinationBlob = destinationBlobContainer.GetBlockBlobReference(blobName);
            //await destinationBlob.FetchAttributesAsync(); // Do we need to fetch given we're overwriting?

            destinationBlob.Metadata.Clear();
            foreach (var metadata in sourceBlob.Metadata)
            {
                destinationBlob.Metadata.Add(metadata.Key, metadata.Value);
            }
            await destinationBlob.SetMetadataAsync();
        }

        private static CloudBlobContainer GetSourceBlobContainer()
        {
            return GetBlobContainer(Environment.GetEnvironmentVariable("SourceConnectionString"), Environment.GetEnvironmentVariable("SourceContainerName"));
        }

        private static CloudBlobContainer GetDestinationBlobContainer()
        {
            return GetBlobContainer(Environment.GetEnvironmentVariable("DestinationConnectionString"), Environment.GetEnvironmentVariable("DestinationContainerName"));
        }

        private static CloudBlobContainer GetBlobContainer(string connectionString, string containerName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference(containerName);
            return cloudBlobContainer;
        }

        private static CloudQueue GetQueue(string queueName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("CopyBlobMetadataQueueConnectionString"));
            CloudQueueClient client = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = client.GetQueueReference(queueName);
            queue.CreateIfNotExistsAsync();
            return queue;
        }
    }
}