using DriverSimulator.BgServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RideHailing.BuildingBlocks.Data;
using RideHailing.BuildingBlocks.Data.Interfaces;
using RideHailing.BuildingBlocks.Outbox;
using RideHailing.Contracts.Setup;

var builder = Host.CreateApplicationBuilder(args);

var connectionString =
    "Server=localhost;" +
    "Database=RideServiceDb;" +
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