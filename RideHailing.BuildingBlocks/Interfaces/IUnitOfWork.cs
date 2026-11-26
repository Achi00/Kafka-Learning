using System.Data.Common;

namespace RideHailing.BuildingBlocks.Interfaces
{
    public interface IUnitOfWork : IAsyncDisposable
    {
        DbConnection Connection { get; }
        DbTransaction Transaction { get; }
        Task BeginAsync(CancellationToken ct);
        Task CommitAsync(CancellationToken ct);
    }
}