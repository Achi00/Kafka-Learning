using DriverSimulator.BgServices;
using DriverSimulator.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RideHailing.BuildingBlocks;
using RideHailing.BuildingBlocks.Interfaces;
using RideHailing.Contracts.Setup;

var builder = Host.CreateApplicationBuilder(args);

var connectionString =
    "Server=localhost;" +
    "Database=Kafka_Learning;" +
    "Integrated Security=True;" +
    "TrustServerCertificate=True;";

// producer's singleton services
builder.Services.AddKafka();
// simulated bg worker
builder.Services.AddHostedService<DriverSimulatorWorker>();
// connection factory
builder.Services.AddSingleton<IDbConnectionFactory>(new SqlConnectionFactory(connectionString));
// outbox repository
builder.Services.AddScoped<OutboxRepository>();
builder.Services.AddScoped<IUnitOfWork, UnitOfWork>();


var host = builder.Build();

var kafka = host.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;

await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);

await host.RunAsync();