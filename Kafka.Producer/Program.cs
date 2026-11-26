using Kafka.Producer.BgServices;
using Kafka.Producer.Database;
using Kafka.Producer.Outbox;
using Kafka.Shared.Setup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

var connectionString =
    "Server=localhost;" +
    "Database=Kafka_Learning;" +
    "Integrated Security=True;" +
    "TrustServerCertificate=True;";

// producer's singleton services
builder.Services.AddKafka();
// simulated bg worker
builder.Services.AddHostedService<DriverSimulator>();
// connection factory
builder.Services.AddSingleton<IDbConnectionFactory>(new SqlConnectionFactory(connectionString));
// outbox repository
builder.Services.AddScoped<OutboxRepository>();
builder.Services.AddScoped<IUnitOfWork, UnitOfWork>();


var host = builder.Build();

var kafka = host.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;

await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);

await host.RunAsync();