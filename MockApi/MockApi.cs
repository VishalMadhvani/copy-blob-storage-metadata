using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Threading;

namespace CopyBlobMetadata
{
    public static class MockApi
    {
        [FunctionName("MockApi")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            var table = await GetTable();
            var queryString = req.GetQueryParameterDictionary();

            if (!queryString.ContainsKey("ContinuationToken"))
            {
                var continueCount = queryString.ContainsKey("ContinueCount") ? int.Parse(queryString["ContinueCount"]) : 1;
                var responseDelay = queryString.ContainsKey("ResponseDelay") ? int.Parse(queryString["ResponseDelay"]) : 1000;
                var newContinuationToken = Guid.NewGuid().ToString();
                var newEntity = new MockApiEntity() { PartitionKey = newContinuationToken, RowKey = newContinuationToken,  ContinueCount = continueCount, ContinueRemaining = continueCount, ResponseDelay = responseDelay };
                var tableInsertResult = await table.ExecuteAsync(TableOperation.Insert(newEntity));
                newEntity = tableInsertResult.Result as MockApiEntity;
                Thread.Sleep(responseDelay);
                return new OkObjectResult(new { ContinuationToken = newEntity.PartitionKey });
            }

            var continuationToken = queryString["ContinuationToken"];
            var tableRetrieveResult = await table.ExecuteAsync(TableOperation.Retrieve<MockApiEntity>(continuationToken, continuationToken));
            var entity = tableRetrieveResult.Result as MockApiEntity;

            if (entity == null)
                return new NotFoundResult();

            if (entity.ContinueRemaining == 0)
            {
                return new OkObjectResult(new { ContinuationToken = "null" });
            }

            entity.ContinueRemaining--;
            await table.ExecuteAsync(TableOperation.Replace(entity));
            Thread.Sleep(entity.ResponseDelay);
            return new OkObjectResult(new { ContinuationToken = entity.PartitionKey });
        }

        public class MockApiEntity : TableEntity
        {
            public int ContinueCount { get; set; }
            public int ResponseDelay { get; set; }
            public int ContinueRemaining { get; set; }
        }

        private static async Task<CloudTable> GetTable()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            CloudTableClient client = storageAccount.CreateCloudTableClient();
            CloudTable table = client.GetTableReference("MockApi");
            if (!await table.ExistsAsync())
                await table.CreateAsync();
            return table;
        }
    }
}
