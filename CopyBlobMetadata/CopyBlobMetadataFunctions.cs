using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;

namespace blobmetadataupdate
{
    public static class CopyBlobMetadataFunctions
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly string functionAppUrl = Environment.GetEnvironmentVariable("FunctionAppUrl");
        private static readonly string functionStorageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

        [FunctionName("Start")]
        public static async Task<IActionResult> Start(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            var copyMetadataInfo = await CopyMetadataInfo.Deserialize(req.Body);
            copyMetadataInfo.ExecutionId = Guid.NewGuid().ToString();

            if (string.IsNullOrWhiteSpace(copyMetadataInfo.SourceConnectionString)) throw new ArgumentNullException("SourceConnectionString");
            if (string.IsNullOrWhiteSpace(copyMetadataInfo.SourceContainerName)) throw new ArgumentNullException("SourceContainerName");
            if (string.IsNullOrWhiteSpace(copyMetadataInfo.DestinationConnectionString)) throw new ArgumentNullException("DestinationConnectionString");
            if (string.IsNullOrWhiteSpace(copyMetadataInfo.DestinationContainerName)) throw new ArgumentNullException("DestinationContainerName");

            await EnsureInfrastructure();

            _ = httpClient.PostAsync($"{functionAppUrl}GetBlobs", new StringContent(copyMetadataInfo.ToString()));
            return new OkResult();
        }

        [FunctionName("GetBlobs")]
        public static async Task GetBlobs(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            req.GetQueryParameterDictionary().TryGetValue("ContinuationToken", out var continuationToken);
            var copyMetadataInfo = await CopyMetadataInfo.Deserialize(req.Body);
            var blobContainer = GetBlobContainer(copyMetadataInfo.SourceConnectionString, copyMetadataInfo.SourceContainerName);

            var results = await blobContainer.ListBlobsSegmentedAsync(null, true, BlobListingDetails.None, 5000, new BlobContinuationToken() { NextMarker = continuationToken }, null, null);

            continuationToken = results.ContinuationToken?.NextMarker;
            if (!string.IsNullOrWhiteSpace(continuationToken))
            {
                _ = httpClient.PostAsync($"{functionAppUrl}GetBlobs?ContinuationToken={continuationToken}", new StringContent(copyMetadataInfo.ToString()));
            }

            List<CloudBlockBlob> blobs = results.Results.OfType<CloudBlockBlob>().ToList();
            var tracker = 0;
            while (tracker < blobs.Count)
            {
                var take = blobs.Count - tracker > 500 ? 500 : blobs.Count - tracker;
                var pagedResults = blobs.Skip(tracker).Take(take);
                var jsonContent = JsonConvert.SerializeObject(pagedResults.Select(blob => new CopyMetadataInfo()
                {
                    ExecutionId = copyMetadataInfo.ExecutionId,
                    BlobName = blob.Name,
                    //Metadata = blob.Metadata.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                    SourceConnectionString = copyMetadataInfo.SourceConnectionString,
                    SourceContainerName = copyMetadataInfo.SourceContainerName,
                    DestinationConnectionString = copyMetadataInfo.DestinationConnectionString,
                    DestinationContainerName = copyMetadataInfo.DestinationContainerName,
                }));
                var httpContent = new StringContent(jsonContent);
                _ = httpClient.PostAsync($"{functionAppUrl}ProcessSegment", httpContent);
                tracker += take;
            }
        }

        [FunctionName("ProcessSegment")]
        public static async Task ProcessSegments(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            var copyMetadataQueue = GetCopyMetadataQueue();
            using (StreamReader streamReader = new StreamReader(req.Body))
            {
                var json = await streamReader.ReadToEndAsync();
                var copyMetadataInfos = JsonConvert.DeserializeObject<List<CopyMetadataInfo>>(json);
                var enqueueTasks = copyMetadataInfos
                    .Select(copyMetadataInfo =>
                        copyMetadataQueue.AddMessageAsync(new CloudQueueMessage(copyMetadataInfo.ToString())))
                    .ToList();
                await Task.WhenAll(enqueueTasks);
            }
        }

        [FunctionName("CopyMetadata")]
        public static async Task UpdateMetadata(
            [QueueTrigger("copymetadata")]string json,
            ILogger log)
        {
            var copyMetadataInfo = CopyMetadataInfo.Deserialize(json);

            var sourceBlob =
                GetBlobContainer(copyMetadataInfo.SourceConnectionString, copyMetadataInfo.SourceContainerName)
                .GetBlockBlobReference(copyMetadataInfo.BlobName);
            await sourceBlob.FetchAttributesAsync();

            if (!sourceBlob.Metadata.Any())
            {
                return;
            }

            var destinationBlob =
                GetBlobContainer(copyMetadataInfo.DestinationConnectionString, copyMetadataInfo.DestinationContainerName)
                .GetBlockBlobReference(copyMetadataInfo.BlobName);
            //await destinationBlob.FetchAttributesAsync(); // No need to fetch as being replaced.

            destinationBlob.Metadata.Clear();
            foreach (var metadata in sourceBlob.Metadata)
            {
                destinationBlob.Metadata.Add(metadata.Key, metadata.Value);
            }
            await destinationBlob.SetMetadataAsync();
        }

        public class CopyMetadataInfo
        {
            public string ExecutionId { get; set; }
            public string BlobName { get; set; }
            public Dictionary<string, string> Metadata { get; set; }
            public string SourceConnectionString { get; set; }
            public string SourceContainerName { get; set; }
            public string DestinationConnectionString { get; set; }
            public string DestinationContainerName { get; set; }

            public static async Task<CopyMetadataInfo> Deserialize(Stream stream)
            {
                using (StreamReader streamReader = new StreamReader(stream))
                {
                    var json = await streamReader.ReadToEndAsync();
                    return Deserialize(json);
                }
            }

            public static CopyMetadataInfo Deserialize(string json)
            {
                return JsonConvert.DeserializeObject<CopyMetadataInfo>(json) ?? new CopyMetadataInfo();
            }

            public static string Serialize(CopyMetadataInfo copyMetadataInfo)
            {
                return JsonConvert.SerializeObject(copyMetadataInfo);
            }

            public override string ToString()
            {
                return Serialize(this);
            }
        }

        private static CloudBlobContainer GetBlobContainer(string connectionString, string containerName)
        {
            return
                CloudStorageAccount.Parse(connectionString)
                .CreateCloudBlobClient()
                .GetContainerReference(containerName);
        }

        private static CloudQueue GetCopyMetadataQueue()
        {
            return
                CloudStorageAccount.Parse(functionStorageConnectionString)
                .CreateCloudQueueClient()
                .GetQueueReference("copymetadata");
        }

        private static async Task EnsureInfrastructure()
        {
            var copyMetadataQueue = GetCopyMetadataQueue();
            if (!await copyMetadataQueue.ExistsAsync())
                await copyMetadataQueue.CreateAsync();
        }
    }
}