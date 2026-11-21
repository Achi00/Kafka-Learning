using Confluent.Kafka;
using Kafka.Shared.Setup;

await KafkaConfig.EnsureTopicAsync(KafkaDefaults.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);

var config = new ProducerConfig
{
    BootstrapServers = KafkaDefaults.BootstrapServers
};

using var producer = new ProducerBuilder<string, string>(config).Build();

while (true)
{
    var random = new Random();
    var driverKey = $"driver-{random.Next(1, 5)}";

    var lat = random.NextDouble() * 100;
    var lng = random.NextDouble() * 100;

    var value = $"{lat},{lng}";

    var res = await producer.ProduceAsync(Topics.DriverLocations, new Message<string, string> { Key = driverKey, Value = value });

    Console.WriteLine(res.Message.Key);
    Console.WriteLine(res.Partition.Value);
    Console.WriteLine(res.Offset.Value);

    await Task.Delay(500);
}