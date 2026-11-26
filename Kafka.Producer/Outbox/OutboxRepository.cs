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
    }
}
