using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace AzureTableTest
{
    class Program
    {
        const string TableName = "SpeedTestTable20140926";
        const int TestCount = 10;

        static void Main()
        {
            var entities = CreateEntities();
            InsertEntities(entities);
            CheckSpeed();
        }

        private static List<SampleEntity> CreateEntities()
        {
            var entities = new List<SampleEntity>();
            for (var i = 0; i < TestEntitiesCounts.Max(); i++)
            {
                var entity = new SampleEntity
                {
                    PartitionKey = "AzureTableTest",
                    RowKey = Guid.NewGuid().ToString(),
                    Property1 = Guid.NewGuid().ToString(),
                    Property2 = Guid.NewGuid().ToString(),
                    Property3 = Guid.NewGuid().ToString(),
                    Property4 = Guid.NewGuid().ToString(),
                    Property5 = DateTime.UtcNow,
                };
                entities.Add(entity);
            }
            return entities;
        }

        private static void InsertEntities(List<SampleEntity> entities)
        {
            var table = GetTableReference();

            var index = 0;
            while (true)
            {
                var insertEntities = entities.Skip(index).Take(100).ToList();
                if (!insertEntities.Any()) return;

                var batch = new TableBatchOperation();
                foreach (var insertEntity in insertEntities)
                {
                    var operation = TableOperation.Insert(insertEntity);
                    batch.Add(operation);
                }
                table.ExecuteBatch(batch);

                index += 100;
            }
        }

        private static void CheckSpeed()
        {
            var results = new List<string>();

            var table = GetTableReference();
            var query = new TableQuery<SampleEntity>();
            for (var i = 0; i < TestCount; i++)
            {
                foreach (var entitiesCount in TestEntitiesCounts)
                {
                    var stopwatch = new Stopwatch();
                    stopwatch.Start();
                    table.ExecuteQuery(query).Take(entitiesCount).ToList();
                    stopwatch.Stop();
                    results.Add(string.Format("{0},{1}", entitiesCount, stopwatch.ElapsedMilliseconds));
                }
                File.WriteAllLines(@"result.txt", results, Encoding.UTF8);
            }
        }

        private static IEnumerable<int> TestEntitiesCounts
        {
            get
            {
                return new List<int>
                {
                    10,
                    100,
                    500,
                    1000,
                    2500,
                    5000,
                    7500,
                    10000,
                    25000,
                    50000,
                    75000,
                    100000,
                };
            }
        }

        private static CloudTable GetTableReference()
        {
            const string connectionString = "{Azureストレージアカウントの接続文字列}";
            var account = CloudStorageAccount.Parse(connectionString);
            var client = account.CreateCloudTableClient();
            var table = client.GetTableReference(TableName);
            table.CreateIfNotExists();
            return table;
        }
    }

    class SampleEntity : TableEntity
    {
        public string Property1 { get; set; }
        public string Property2 { get; set; }
        public string Property3 { get; set; }
        public string Property4 { get; set; }
        public DateTime Property5 { get; set; }
    }
}
