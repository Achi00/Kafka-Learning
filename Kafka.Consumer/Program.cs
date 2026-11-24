using Confluent.Kafka;
using Kafka.Consumer;
using Kafka.Consumer.Handlers;
using Kafka.Consumer.Storege;
using Kafka.Shared.Setup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

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
await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.DeadLetter(Topics.DriverLocations, consumerOptions.GroupId), 1);
await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.Retry(Topics.DriverLocations, consumerOptions.GroupId), Topics.DriverLocationsPartitions);

await host.RunAsync();