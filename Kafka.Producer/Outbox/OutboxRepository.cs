using Kafka.Producer.Database;

namespace Kafka.Producer.Outbox
{
    public sealed class OutboxRepository
    {
        private readonly IDbConnectionFactory _connectionFactory;

        public OutboxRepository(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        public async Task AddAsync(OutboxMessage message)
        {
            using var connection = _connectionFactory.CreateConnection();

            const string sql = """
                INSERT INTO OutboxMessages 
                    (MessageId, Topic, MessageKey, Type, Payload, CreatedAt, ProcessedAt, Attempts)
                VALUES ()
                """;
        }
    }
}
