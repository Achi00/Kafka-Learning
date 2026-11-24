using Confluent.Kafka;
using Kafka.Producer.BgServices;
using Kafka.Shared.Setup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

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
builder.Services.AddHostedService<DriverSimulator>();