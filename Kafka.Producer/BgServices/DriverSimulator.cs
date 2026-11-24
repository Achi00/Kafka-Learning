using Confluent.Kafka;
using Kafka.Shared.Setup;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.Producer.BgServices
{
    public sealed class DriverSimulator : BackgroundService
    {
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<DriverSimulator> _logger;
        public DriverSimulator(IProducer<string, string> producer, ILogger<DriverSimulator> logger)
        {
            _producer = producer;
            _logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                //var result = await producer.ProduceAsync(Topics.DriverLocations, )
            }
        }
    }
}
