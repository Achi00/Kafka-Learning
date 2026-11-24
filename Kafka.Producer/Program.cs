using Confluent.Kafka;
using Kafka.Producer.BgServices;
using Kafka.Shared.Setup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

Console.WriteLine(builder.Environment.ContentRootPath);

Console.WriteLine(
    builder.Configuration["Kafka:BootstrapServers"] ?? "NULL"
);

// producer's singleton services
builder.Services.AddKafka();
// simulated bg worker
builder.Services.AddHostedService<DriverSimulator>();

var host = builder.Build();

var kafka = host.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;

Console.WriteLine(kafka.BootstrapServers);
await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);


await host.RunAsync();