using Kafka.Shared.Setup;

namespace Kafka.Consumer.Handlers
{
    // bussines logic
    public sealed class DriverLocationHandler : IDriverLocationHandler
    {
        public Task HandleAsync(DriverLocation location, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}
