using RideHailing.BuildingBlocks.Interfaces;
using System.Data.Common;

namespace RideHailing.BuildingBlocks
{
    public sealed class UnitOfWork : IUnitOfWork
    {
        private readonly IDbConnectionFactory _connectionFactory;
        private DbConnection? _connection;
        private DbTransaction? _transaction;

        public DbConnection Connection => _connection ?? throw new InvalidOperationException("Call BeginAsync first.");
        public DbTransaction Transaction => _transaction ?? throw new InvalidOperationException("Call BeginAsync first.");

        public UnitOfWork(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;            
        }

        public async Task BeginAsync(CancellationToken ct)
        {
            _connection = _connectionFactory.CreateConnection();
            await _connection.OpenAsync(ct);
            _transaction = await _connection.BeginTransactionAsync(ct);
        }

        public Task CommitAsync(CancellationToken ct)
        {
            return Transaction.CommitAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            if (_connection is not null)
            {
                // not committed => roll back
                await _connection.DisposeAsync();
            }
            if (_transaction is not null)
            {
                // back to connection pool
                await _transaction.DisposeAsync();
            }
        }
    }
}
