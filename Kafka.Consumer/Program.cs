using Confluent.Kafka;
using Kafka.Shared.Setup;

await KafkaConfig.EnsureTopicAsync(KafkaDefaults.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);

var config = new ConsumerConfig
{
    BootstrapServers = KafkaDefaults.BootstrapServers,
    GroupId = "dispatch",
    AutoOffsetReset = AutoOffsetReset.Earliest
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

try
{
    while (true)
    {
        var cr = consumer.Consume(cts.Token);   // blocks until a message arrives (or cancel)
        Console.WriteLine($"{cr.Message.Key} | partition {cr.Partition.Value} | offset {cr.Offset.Value} | {cr.Message.Value}");
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