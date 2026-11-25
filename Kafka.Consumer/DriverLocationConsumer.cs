using Confluent.Kafka;
using Kafka.Consumer.Handlers;
using Kafka.Shared.Setup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;

namespace Kafka.Consumer
{
    public sealed class DriverLocationConsumer(
        IOptions<KafkaOptions> options,
        IServiceScopeFactory scopeFactory,
        IOptions<DriverLocationConsumerOptions> driverOptions,
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
                GroupId = driverOptions.Value.GroupId,
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

            consumer.Subscribe(new[] { Topics.DriverLocations, Topics.Retry(driverOptions.Value.GroupId, Topics.DriverLocations) });

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

                    // buissness logic handler and offset commiting
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
            DriverLocation location;
            try
            {
                location = JsonSerializer.Deserialize<DriverLocation>(cr.Message.Value, JsonOptions)
                           ?? throw new JsonException("Payload deserialized to null");

                // if deserialized as valid json check values itself
                Validate(location);
            }
            catch (Exception ex) when (ex is JsonException or ValidationException)
            {
                // Permanent failure -> DLQ. If the DLQ write itself throws, we let it bubble:
                // offset is NOT stored, the host stops, and the message is redelivered after restart.
                deadLetters.PublishAsync(cr, ex, ct).GetAwaiter().GetResult();
                return;
            }

            using var scope = scopeFactory.CreateScope();
            // get main bussines logic handler
            var handler = scope.ServiceProvider.GetRequiredService<IDriverLocationHandler>();
            // dont using await, using getawaiter because we are in consumers dedicated thread, we have no synchronization context, cant deadlock
            // if we use await thread will be released and be hadled by threadpool
            handler.HandleAsync(location, ct).GetAwaiter().GetResult();
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

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            // missing ctor param = JsonException
            RespectRequiredConstructorParameters = true,
            // null in non-nullable property = JsonException
            RespectNullableAnnotations = true 
        };
        // validate coordinates, if it is 0 it means it is filled as default integlar value bacause of wrong json payload, reject it
        private static void Validate(DriverLocation l)
        {
            if (l.EventId == Guid.Empty)
            {
                throw new ValidationException("EventId is empty");
            }
            if (string.IsNullOrWhiteSpace(l.DriverId))
            {
                throw new ValidationException("DriverId is empty");
            }
            if (l.Lat is < -90 or > 90)
            {
                throw new ValidationException($"Lat out of range: {l.Lat}");
            }
            if (l.Lng is < -180 or > 180)
            {
                throw new ValidationException($"Lng out of range: {l.Lng}");
            }
        }
    }
}
