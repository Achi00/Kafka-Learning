using Dapper;
using Kafka.Producer.Database;
using RideHailing.BuildingBlocks.Interfaces;

namespace Kafka.Producer.Outbox
{
    public sealed class OutboxRepository
    {
        private readonly IUnitOfWork _unitOfWork;

        public OutboxRepository(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public Task AddAsync(OutboxMessage message)
        {
            const string sql = """
                INSERT INTO dbo.OutboxMessages (MessageId, Topic, MessageKey, Type, Payload)
                VALUES (@MessageId, @Topic, @MessageKey, @Type, @Payload)
             """;

            return _unitOfWork.Connection.ExecuteAsync(sql, message, _unitOfWork.Transaction);
        }
    }
}
