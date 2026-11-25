using Confluent.Kafka;
using Dapper;
using Kafka.Producer.BgServices;
using Kafka.Shared.Setup;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

// producer's singleton services
builder.Services.AddKafka();
// simulated bg worker
builder.Services.AddHostedService<DriverSimulator>();

var host = builder.Build();

var kafka = host.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;

Console.WriteLine(kafka.BootstrapServers);
await KafkaConfig.EnsureTopicAsync(kafka.BootstrapServers, Topics.DriverLocations, Topics.DriverLocationsPartitions);

var connectionString =
    "Server=localhost;" +
    "Database=KafkaOutboxDemo;" +
    "Integrated Security=True;" +
    "TrustServerCertificate=True;";

await using var connection = new SqlConnection(connectionString);

await connection.OpenAsync();
Console.WriteLine("Connected to SQL Server!");

var result = await connection.QuerySingleAsync<int>(
    "SELECT 1");

Console.WriteLine($"Database returned: {result}");

await host.RunAsync();