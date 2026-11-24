using Confluent.Kafka;
using Kafka.Consumer;
using Kafka.Consumer.Handlers;
using Kafka.Consumer.Storege;
using Kafka.Shared.Setup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddOptions<KafkaOptions>()
    .BindConfiguration(KafkaOptions.Section)
    .ValidateDataAnnotations()
    .ValidateOnStart();

// producer singleton service
builder.Services.AddSingleton<IProducer<string, string>>(sp => 
{
    var o = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = o.BootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true
    }).Build();
});

builder.Services.AddSingleton<IDriverLocationStore, InMemoryDriverLocationStore>();
builder.Services.AddScoped<IDriverLocationHandler, DriverLocationHandler>();
builder.Services.AddSingleton<DeadLetterPublisher>();
builder.Services.AddHostedService<DriverLocationConsumer>();

var host = builder.Build();

var kafka = host.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;
await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);
await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.DriverLocationsDlq, 1);

await host.RunAsync();