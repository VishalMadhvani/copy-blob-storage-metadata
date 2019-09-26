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
    public static class HybridFunction
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

        private static CloudQueue GetQueue()
        {
            //CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageAccount"));
            //CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            CloudQueueClient client = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = client.GetQueueReference("data");
            return queue;
        }

        [FunctionName("HybridOrchestrator")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context,
            ILogger log)
        {
            List<Task> queueBlobNamesFromSegmentActivity = new List<Task>();
            BlobContinuationToken blobContinuationToken = null;
            while (true)
            {
                var response = await context.CallActivityAsync<Tuple<BlobContinuationToken, List<string>>>("GetBlobSegmentsActivity", blobContinuationToken);
                //List<CloudBlockBlob> cloudBlockBlobs = response.Results.OfType<CloudBlockBlob>().ToList();
                queueBlobNamesFromSegmentActivity.Add(context.CallActivityAsync("QueueBlobNamesFromSegmentActivity", response.Item2));
                blobContinuationToken = response.Item1;
                if (blobContinuationToken is null) break;
            }

            await Task.WhenAll(queueBlobNamesFromSegmentActivity);
        }

        [FunctionName("GetBlobSegmentsActivity")]
        public static async Task<Tuple<BlobContinuationToken, List<string>>> GetBlobSegmentsActivity([ActivityTrigger] BlobContinuationToken blobContinuationToken, ILogger log)
        {
            CloudBlobContainer cloudBlobContainer = GetBlobContainer();

            var results = await cloudBlobContainer.ListBlobsSegmentedAsync(null, blobContinuationToken);
            return Tuple.Create(results.ContinuationToken, results.Results.OfType<CloudBlockBlob>().Select(blob => blob.Name).ToList());
        }

        [FunctionName("QueueBlobNamesFromSegmentActivity")]
        public static async Task QueueBlobNamesFromSegmentActivity([ActivityTrigger] List<string> cloudBlockBlobNames, ILogger log)
        {
            CloudQueue queue = GetQueue();

            foreach (string blobName in cloudBlockBlobNames)
            {
                await queue.AddMessageAsync(new CloudQueueMessage(blobName));
            }
        }

        [FunctionName("HybridOrchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("HybridOrchestrator", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}