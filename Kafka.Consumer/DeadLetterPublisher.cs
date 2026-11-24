using Confluent.Kafka;
using Kafka.Shared.Setup;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text;

namespace Kafka.Consumer
{
    // failed messages
    public sealed class DeadLetterPublisher
    {
        private readonly IProducer<string, string> _producer;
        private readonly IOptions<KafkaOptions> _kafkaOptions;
        private readonly IOptions<DriverLocationConsumerOptions> _driverOptions;
        private readonly ILogger<DeadLetterPublisher> _logger;
        public DeadLetterPublisher(
            IProducer<string, string> producer,
            IOptions<KafkaOptions> kafkaOptions,
            IOptions<DriverLocationConsumerOptions> driverOptions,
            ILogger<DeadLetterPublisher> logger)
        {
            _producer = producer;
            _kafkaOptions = kafkaOptions;
            _driverOptions = driverOptions;
            _logger = logger;
        }
        public async Task PublishAsync(ConsumeResult<string, string> cr, Exception error, CancellationToken ct)
        {
            // kafka headers for metadata
            var headers = new Headers();

            if (cr.Message.Headers is not null)
            {
                foreach (var header in headers)
                {
                    headers.Add(header.Key, header.GetValueBytes());
                }
            }

            Add(headers, "dlq-original-topic", cr.Topic);
            Add(headers, "dlq-original-partition", cr.Partition.Value.ToString());
            Add(headers, "dlq-original-offset", cr.Offset.Value.ToString());
            Add(headers, "dlq-consumer-group", _driverOptions.Value.GroupId);
            Add(headers, "dlq-error-type", error.GetType().FullName ?? "unknown");
            Add(headers, "dlq-error-message", error.Message);
            Add(headers, "dlq-failed-at", DateTimeOffset.UtcNow.ToString("O"));

            // produce message, even if it is for dead queue it is only way to move it away based on topic!!!
            await _producer.ProduceAsync(Topics.DriverLocationsDlq, new Message<string, string>
            {
                Key = cr.Message.Key,
                Value = cr.Message.Value,
                Headers = headers
            }, ct);

            _logger.LogWarning("Sent to DLQ: p{Partition} o{Offset} ({Error})",
                cr.Partition.Value, cr.Offset.Value, error.Message);
        }
        // helper method to add header metadata with correct valie and key
        private static void Add(Headers h, string key, string value) => h.Add(key, Encoding.UTF8.GetBytes(value));
    }
}
