using Confluent.Kafka;
using Dapper;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RideHailing.BuildingBlocks.Data.Interfaces;
using System.ComponentModel;
using System.Data.Common;

namespace RideHailing.BuildingBlocks.Outbox
{
    public sealed class OutboxRelay : BackgroundService
    {
        private readonly IDbConnectionFactory _connectionFactory;
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<OutboxRelay> _logger;

        // relay configuration fields
        private const string LockName = "outbox-relay";
        private const int BatchSize = 100;
        private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan StandbyDelay = TimeSpan.FromSeconds(5);

        public OutboxRelay(IDbConnectionFactory connectionFactory, IProducer<string, string> producer, ILogger<OutboxRelay> logger)
        {
            _connectionFactory = connectionFactory;
            _producer = producer;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // dedicated connection, lock lives exactly as long as this connection
                    await using var lockConnection = _connectionFactory.CreateConnection();
                    await lockConnection.OpenAsync(stoppingToken);

                    if (!await TryAcquireLockAsync(lockConnection))
                    {
                        // another instance is relaying
                        await Task.Delay(StandbyDelay, stoppingToken);
                        continue;
                    }

                    // lock aquired, publishes batch rows to producer
                    _logger.LogInformation("Acquired outbox relay lock");
                    await RelayLoopAsync(lockConnection, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // cancellation requested, normal exit
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError("Outbox relay failed, retrying");
                    await Task.Delay(StandbyDelay, stoppingToken);
                }
            }
        }

        // tries to aquire application level lock, only one instance can aquire it, others wait
        private static async Task<bool> TryAcquireLockAsync(DbConnection connection)
        {
            var result = await connection.ExecuteScalarAsync<int>("""
            DECLARE @result INT;
            EXEC @result = sp_getapplock
                @Resource = @Resource, 
                @LockMode = 'Exclusive',
                @LockOwner = 'Session', 
                @LockTimeout = 0;
            SELECT @result;
            """, new { Resource = LockName });

            return result >= 0;
        }

        private async Task RelayLoopAsync(DbConnection connection, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                // batches n amount of rows, only gets not processed messages, indexed in db, orderd by secuance for continuety
                var batch = (await connection.QueryAsync<OutboxMessage>("""
                SELECT TOP (@BatchSize)
                       Sequence, MessageId, Topic, MessageKey, Type, Payload,
                       CreatedAt, ProcessedAt, Attempts, LastError
                FROM dbo.OutboxMessages
                WHERE ProcessedAt IS NULL
                ORDER BY Sequence
                """, new { BatchSize })).ToList();

                
            }
        }
    }
}
