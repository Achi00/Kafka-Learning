using Confluent.Kafka;
using Kafka.Shared.Setup;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Kafka.Producer.BgServices
{
    public sealed class DriverSimulator : BackgroundService
    {
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<DriverSimulator> _logger;

        private readonly Random _random = new();
        public DriverSimulator(IProducer<string, string> producer, ILogger<DriverSimulator> logger)
        {
            _producer = producer;
            _logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken ct)
        {
            // randomize same way ase before in producer
            int i = 1;
            while (!ct.IsCancellationRequested)
            {
                // randomize data
                var driverId = $"driver-{_random.Next(1, 6)}";
                var location = new DriverLocation(
                Guid.NewGuid(), driverId,
                _random.NextDouble() * 100, _random.NextDouble() * 100,
                DateTimeOffset.UtcNow);

                await SendAsync(location, ct);

                // every 10th is duplicate
                if (i % 10 == 0)
                {
                    await SendAsync(location, ct);   // duplicate: same event twice
                }

                // every 15th is stele, -10 seconds behind
                if (i % 15 == 0)
                {
                    await SendAsync(location with
                    {
                        EventId = Guid.NewGuid(),
                        Timestamp = DateTimeOffset.UtcNow.AddSeconds(-10)
                    }, ct);
                }

                i++;
                await Task.Delay(500, ct);

            }
        }

        private async Task SendAsync(DriverLocation location, CancellationToken ct)
        {
            var message = new Message<string, string>
            {
                Key = location.DriverId,
                Value = JsonSerializer.Serialize(location)
            };

            var result = await _producer.ProduceAsync(Topics.DriverLocations, message, ct);
            _logger.LogInformation("{DriverId} -> p{Partition} o{Offset}",
                location.DriverId, result.Partition.Value, result.Offset.Value);
        }
    }
}
