using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace blobmetadataupdate
{
    public static class Orchestrator
    {
        [FunctionName("Orchestrator")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context,
            ILogger log)
        {
            // Get list of blobs
            var blobs = await context.CallActivityAsync<List<string>>("GetBlobsActivity", null);

            //// for each blob
            //foreach (var blob in blobs)
            //{
            //    // update blob metadata
            //    await context.CallActivityAsync("UpdateMetadataActivity", blob);
            //}

            var updateBlobMetadataActivities = blobs.Select(blob => context.CallActivityAsync("UpdateMetadataActivity", blob)).ToList();
            if (!context.IsReplaying)
                log.LogInformation($"Started {updateBlobMetadataActivities.Count} Activities");

            await Task.WhenAll(updateBlobMetadataActivities);
        }

        [FunctionName("GetBlobsActivity")]
        public static async Task<List<string>> GetBlobsActivity([ActivityTrigger] string name, ILogger log)
        {
            //return new List<string>() { "blob1", "blob10", "blob100", "blob11000", "blob10000" };
            //CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageAccount"));
            //CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference("data");

            List<string> blobNames = new List<string>();
            BlobContinuationToken blobContinuationToken = null;
            do
            {
                var results = await cloudBlobContainer.ListBlobsSegmentedAsync(null, blobContinuationToken);
                // Get the value of the continuation token returned by the listing call.
                blobContinuationToken = results.ContinuationToken;
                blobNames.AddRange(results.Results.OfType<CloudBlockBlob>().Select(blob => blob.Name));
                //foreach (IListBlobItem item in results.Results)
                //{
                //    blobNames.Add(((CloudBlob)item).Name);
                //}
            } while (blobContinuationToken != null); // Loop while the continuation token is not null.
            log.LogWarning($"Got {blobNames.Count} blobs.");
            return blobNames;
        }

        [FunctionName("UpdateMetadataActivity")]
        public static async Task UpdateMetadataActivity([ActivityTrigger] string name, ILogger log)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageAccount"));
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference("data");
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(name);
            await cloudBlockBlob.FetchAttributesAsync();
            //if (cloudBlockBlob.Metadata.Count > 0)
                cloudBlockBlob.Metadata.Clear();
            //else
                cloudBlockBlob.Metadata.Add("test_key", "test_value");
            await cloudBlockBlob.SetMetadataAsync();
            log.LogWarning($"Updating metadata for {name}");
        }

        [FunctionName("Orchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("Orchestrator", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}