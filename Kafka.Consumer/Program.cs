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

// one per consumer instance, in case of multiple consumer instances this wont work!!!!!
var latest = new Dictionary<string, DriverLocation>();
try
{
    while (true)
    {
        // ConsumerResult is not json
        var cr = consumer.Consume(cts.Token);   // blocks until a message arrives (or cancel)

        DriverLocation? location;
        try
        {
            location = JsonSerializer.Deserialize<DriverLocation>(cr.Message.Value);
        }
        // in case json value is corrupt and throws we will lose offset value commit, so kafka redelivers same message again
        catch (JsonException ex)
        {
            Console.WriteLine($"POISON | partition {cr.Partition.Value} | offset {cr.Offset.Value} | {ex.Message}");
            // skip it later send to dead-letter topic instead of dropping
            consumer.StoreOffset(cr);
            continue;
        }

        // check result
        if (location is not null)
        {
            // if not exists in dictionary or timestamp is future thatn current save in dictionary
            if (!latest.TryGetValue(location.DriverId, out var value) || location.Timestamp > value.Timestamp)
            {
                latest[location.DriverId] = location;
                Console.WriteLine($"APPLIED | {location.DriverId} | p{cr.Partition.Value} o{cr.Offset.Value} | {location.Timestamp:HH:mm:ss.fff}");
            }
            // duplicate value events match
            else if (location.EventId == value.EventId)
            {
                Console.WriteLine($"DUPLICATE | {location.DriverId} | p{cr.Partition.Value} o{cr.Offset.Value} | same event {location.EventId}");
            }
            // staled value in this case -10 seconds
            else
            {
                Console.WriteLine($"STALE | {location.DriverId} | p{cr.Partition.Value} o{cr.Offset.Value} | incoming {location.Timestamp:HH:mm:ss.fff} < current {value.Timestamp:HH:mm:ss.fff}");
            }
        }
        else
        {
            Console.WriteLine("POISON wrong JSON format");
        }
        // only reached if processing finished
        consumer.StoreOffset(cr);
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