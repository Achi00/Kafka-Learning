using Confluent.Kafka;
using Kafka.Shared.Setup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Kafka.Consumer
{
    public sealed class DriverLocationConsumer(
        IOptions<KafkaOptions> options,
        IServiceScopeFactory scopeFactory,
        DeadLetterPublisher deadLetters,
        ILogger<DriverLocationConsumer> logger) : BackgroundService
    {
        // bacause we cona awati consume() it is bloking operation so we will use consumer dedicated thread only outside of thread pool
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Factory.StartNew(() => 
                ConsumeLoop(stoppingToken), 
                stoppingToken,
                // dedicated thread for blocking loop
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        private void ConsumeLoop(CancellationToken ct)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = options.Value.BootstrapServers,
                GroupId = options.Value.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                // at least once, we store after processing
                EnableAutoOffsetStore = false,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            // build consumer
            using var consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, e) =>
                    logger.LogError("Kafka error: {Reason} (fatal: {IsFatal})", e.Reason, e.IsFatal))
                .SetPartitionsAssignedHandler((_, parts) =>
                    logger.LogInformation("Assigned: [{Partitions}]", string.Join(", ", parts.Select(p => p.Partition.Value))))
                .SetPartitionsRevokedHandler((_, parts) =>
                    logger.LogInformation("Revoked: [{Partitions}]", string.Join(", ", parts.Select(p => p.Partition.Value))))
                .Build();

            consumer.Subscribe(Topics.DriverLocations);

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    ConsumeResult<string, string> cr;
                    try
                    {
                        cr = consumer.Consume(ct);
                    }
                    catch (ConsumeException ex) when (!ex.Error.IsFatal)
                    {
                        logger.LogWarning(ex, "Consume error, continuing");
                        continue;
                    }

                    Process(cr, ct);
                    StoreOffset(consumer, cr);
                }
            }
            catch (OperationCanceledException)
            {
                // normal shutdown
            }
            finally
            {
                // commit stored offsets, leave group cleanly
                consumer.Close();
            }
        }

        // now processing contains just deserialization of json
        private void Process(ConsumeResult<string, string> cr, CancellationToken ct)
        {
            DriverLocation? location;
            try
            {
                location = JsonSerializer.Deserialize<DriverLocation>(cr.Message.Value)
                           ?? throw new JsonException("Payload deserialized to null");
            }
            catch (JsonException ex)
            {
                // Permanent failure -> DLQ. If the DLQ write itself throws, we let it bubble:
                // offset is NOT stored, the host stops, and the message is redelivered after restart.
                return;
            }

        }

        // marks message as done, updates per partition
        private void StoreOffset(IConsumer<string, string> consumer, ConsumeResult<string, string> cr)
        {
            try
            {
                consumer.StoreOffset(cr);
            }
            catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_State)
            {
                // Partition was revoked mid-processing; its new owner will reprocess this message.
                logger.LogWarning("Partition {Partition} revoked before storing offset {Offset}",
                    cr.Partition.Value, cr.Offset.Value);
            }
        }
    }
}
