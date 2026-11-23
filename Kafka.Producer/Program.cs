using Confluent.Kafka;
using Kafka.Shared.Setup;
using System.Text.Json;

await KafkaConfig.EnsureTopicAsync(KafkaDefaults.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);

var config = new ProducerConfig
{
    BootstrapServers = KafkaDefaults.BootstrapServers
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var random = new Random();

int i = 0;

while (true)
{
    var driverKey = $"driver-{random.Next(1, 6)}";


    var lat = random.NextDouble() * 100;
    var lng = random.NextDouble() * 100;

    var driverLocation = new DriverLocation(Guid.NewGuid(), driverKey, lat, lng, DateTimeOffset.UtcNow);

    var json = JsonSerializer.Serialize(driverLocation);

    var res = await producer.ProduceAsync(Topics.DriverLocations, new Message<string, string> { Key = driverKey, Value = json });

    // send same message on every 10th
    if (i % 10 == 0)
    {
        await producer.ProduceAsync(Topics.DriverLocations, new Message<string, string> { Key = driverKey, Value = json });
    }

    i++;

    Console.WriteLine(res.Message.Key);
    Console.WriteLine(res.Partition.Value);
    Console.WriteLine(res.Offset.Value);

    await Task.Delay(500);
}