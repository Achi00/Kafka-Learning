using Confluent.Kafka;
using Kafka.Shared.Setup;
using System.Text.Json;

await KafkaConfig.EnsureTopicAsync(KafkaDefaults.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);

var config = new ConsumerConfig
{
    BootstrapServers = KafkaDefaults.BootstrapServers,
    GroupId = args[0],
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,
    // making EnableAutoOffsetStore true can cause message loss if it failed midway
    EnableAutoOffsetStore = false,
};


using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((c, parts) =>
        Console.WriteLine($"ASSIGNED: [{string.Join(", ", parts.Select(p => p.Partition.Value))}]"))
    .SetPartitionsRevokedHandler((c, parts) =>
        Console.WriteLine($"REVOKED:  [{string.Join(", ", parts.Select(p => p.Partition.Value))}]"))
    .Build();

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (sender, e) =>
{
    Console.WriteLine("Sutting down consumer...");
    e.Cancel = true;
    cts.Cancel();
};

consumer.Subscribe(Topics.DriverLocations);

int i = 0;

var latest = new Dictionary<string, DriverLocation>();
try
{
    while (true)
    {
        // ConsumerResult is not json
        var cr = consumer.Consume(cts.Token);   // blocks until a message arrives (or cancel)

        var result = JsonSerializer.Deserialize<DriverLocation>(cr.Message.Value);

        // check result
        if (result is not null)
        {
            // if not exists in dictionary or timestamp is future thatn current save in dictionary
            if (!latest.TryGetValue(result.DriverId, out var value) || result.Timestamp > value.Timestamp)
            {
                latest[result.DriverId] = result;
                Console.WriteLine("APPLIED");
            }
            else
            {
                Console.WriteLine("SKIPPED (stale/duplicate)");
            }
        }
        // only reached if processing finished
        consumer.StoreOffset(cr);


        //Console.WriteLine($"{cr.Message.Key} | partition {cr.Partition.Value} | offset {cr.Offset.Value} | {cr.Message.Value}");
        //Thread.Sleep(1000);

        //i++;
        //if (i >= 15)
        //{
        //    Console.WriteLine($"CRASH while processing partition {cr.Partition.Value} offset {cr.Offset.Value}");
        //    // dies before StoreOffset
        //    Environment.FailFast("Boom");
        //}

    }
}
catch (OperationCanceledException)
{
    // expected on Ctrl+C
}
finally
{
    consumer.Close();
}