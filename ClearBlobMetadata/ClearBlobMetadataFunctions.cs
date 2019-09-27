using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace blobmetadataupdate
{
    public static class ClearBlobMetadataFunctions
    {
        private static CloudBlobContainer GetSourceBlobContainer()
        {
            return GetBlobContainer(Environment.GetEnvironmentVariable("SourceConnectionString"), Environment.GetEnvironmentVariable("SourceContainerName"));
        }

        private static CloudBlobContainer GetBlobContainer(string connectionString, string containerName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference(containerName);
            return cloudBlobContainer;
        }

        private static CloudQueue GetQueue()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("CopyBlobMetadataQueueConnectionString"));
            CloudQueueClient client = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = client.GetQueueReference("copyblobmetadata");
            return queue;
        }

        public class CopyBlobMetadataInput
        {
            public string Source { get; set; }
            public string Destination { get; set; }
        }

        [FunctionName("CopyBlobMetadata")]
        public static async Task CopyBlobMetadata(
            [OrchestrationTrigger] DurableOrchestrationContext context,
            ILogger log)
        {
            //var input = context.GetInput<CopyBlobMetadataInput>();
            List<Task> queueIndividualBlobNamesFromSegmentTasks = new List<Task>();
            BlobContinuationToken blobContinuationToken = null;
            while (true)
            {
                // Verify: Is null a value activity input and hence is the response correctly cached?
                // TODO: Use string continuationToken
                var response = await context.CallActivityAsync<GetBlobNamesInSegmentsOutput>("GetBlobNamesInSegments", blobContinuationToken);

                Task task = context.CallActivityAsync("QueueIndividualBlobNamesFromSegment", response.BlobNames);
                queueIndividualBlobNamesFromSegmentTasks.Add(task);

                blobContinuationToken = response.BlobContinuationToken;
                if (blobContinuationToken is null) break;
            }

            await Task.WhenAll(queueIndividualBlobNamesFromSegmentTasks);

            // TODO: wait until UpdateMetadata functions have completed so Status can be tracked
            //      wait until queue is empty and stable? 
        }

        public class GetBlobNamesInSegmentsOutput
        {
            public BlobContinuationToken BlobContinuationToken { get; set; }
            public List<string> BlobNames { get; set; }
        }

        // TODO: InputDto
        [FunctionName("GetBlobNamesInSegments")]
        public static async Task<GetBlobNamesInSegmentsOutput> GetBlobNamesInSegments(
            [ActivityTrigger] BlobContinuationToken blobContinuationToken,
            ILogger log)
        {
            CloudBlobContainer cloudBlobContainer = GetSourceBlobContainer();

            // TODO: Review docs on available overloads for this method.
            //      What does useFlatBlobListing do?
            //      find a good batch size
            var results = await cloudBlobContainer.ListBlobsSegmentedAsync(null, blobContinuationToken);

            // Will OfType actually filter to block blobs?
            List<string> blobNames = results.Results.OfType<CloudBlockBlob>().Select(blob => blob.Name).ToList();

            var output = new GetBlobNamesInSegmentsOutput()
            {
                BlobContinuationToken = results.ContinuationToken,
                BlobNames = blobNames
            };
            return output;
        }

        // Can this be a SubOrchestrator?
        [FunctionName("QueueIndividualBlobNamesFromSegment")]
        public static async Task QueueIndividualBlobNamesFromSegment(
            [ActivityTrigger] List<string> blobNames,
            ILogger log)
        {
            CloudQueue queue = GetQueue();

            foreach (string blobName in blobNames)
            {
                await queue.AddMessageAsync(new CloudQueueMessage(blobName));
            }
        }

        [FunctionName("Start")]
        public static async Task<HttpResponseMessage> Start(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {
            //var options = JsonConvert.DeserializeObject<JObject>(await req.Content.ReadAsStringAsync());
            //var copyBlobMetadataInput = new CopyBlobMetadataInput()
            //{
            //    Source = options.Value<string>("source"),
            //    Destination = options.Value<string>("destination")
            //};

            string instanceId = await starter.StartNewAsync("CopyBlobMetadata", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        // Can this be an Activity?
        [FunctionName("ClearMetadata")]
        public static async Task UpdateMetadata(
            [QueueTrigger("copyblobmetadata", Connection = "CopyBlobMetadataQueueConnectionString")]string blobName,
            ILogger log)
        {
            var sourceBlobContainer = GetSourceBlobContainer();
            var sourceBlob = sourceBlobContainer.GetBlockBlobReference(blobName);

            //await destinationBlob.FetchAttributesAsync(); // Do we need to fetch given we're overwriting?
            //sourceBlob.Metadata.Clear();
            await sourceBlob.SetMetadataAsync();
        }
    }
}