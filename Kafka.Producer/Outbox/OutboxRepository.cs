using Dapper;
using Kafka.Producer.Database;
using System.Data.Common;

namespace Kafka.Producer.Outbox
{
    public sealed class OutboxRepository
    {
        private readonly IDbConnectionFactory _connectionFactory;

        public OutboxRepository(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        public Task AddAsync(OutboxMessage message, DbConnection connection, DbTransaction transaction)
        {
            const string sql = """
                INSERT INTO dbo.OutboxMessages (MessageId, Topic, MessageKey, Type, Payload)
                VALUES (@MessageId, @Topic, @MessageKey, @Type, @Payload)
             """;

            return connection.ExecuteAsync(sql, message, transaction);
        }
    }
}
