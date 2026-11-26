using System.Data.Common;

namespace Kafka.Producer.Database
{
    public interface IUnitOfWork : IAsyncDisposable
    {
        DbConnection Connection { get; }
        DbTransaction Transaction { get; }
        Task BeginAsync(CancellationToken ct);
        Task CommitAsync(CancellationToken ct);
    }
}