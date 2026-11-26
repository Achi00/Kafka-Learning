using Confluent.Kafka;
using LocationService;
using LocationService.Handlers;
using LocationService.Storege;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RideHailing.Contracts.Setup;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafka();
builder.Services.AddOptions<DriverLocationConsumerOptions>()
    .BindConfiguration(DriverLocationConsumerOptions.Section)
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<IDriverLocationStore, InMemoryDriverLocationStore>();
builder.Services.AddScoped<IDriverLocationHandler, DriverLocationHandler>();
builder.Services.AddSingleton<DeadLetterPublisher>();
builder.Services.AddHostedService<DriverLocationConsumer>();

var host = builder.Build();

var kafka = host.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;
var consumerOptions = host.Services.GetRequiredService<IOptions<DriverLocationConsumerOptions>>().Value;

await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);
// dynamic changinf dlq and retry
await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.DeadLetter(groupId: consumerOptions.GroupId, topic: Topics.DriverLocations), 1);
await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.Retry(groupId: consumerOptions.GroupId, topic: Topics.DriverLocations), Topics.DriverLocationsPartitions);

await host.RunAsync();