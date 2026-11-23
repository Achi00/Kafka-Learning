using Kafka.Shared.Setup;

namespace Kafka.Consumer.Handlers
{
    public interface IDriverLocationHandler
    {
        Task HandleAsync(DriverLocation location, CancellationToken ct);
    }
}