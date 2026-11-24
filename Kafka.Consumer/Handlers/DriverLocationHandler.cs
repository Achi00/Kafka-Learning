using Kafka.Shared.Setup;
using Microsoft.Extensions.Logging;

namespace Kafka.Consumer.Handlers
{
    // bussines logic
    public sealed class DriverLocationHandler : IDriverLocationHandler
    {
        private readonly IDriverLocationStore _locationStore;
        private readonly ILogger<DriverLocationHandler> _logger;

        public DriverLocationHandler(IDriverLocationStore locationStore, ILogger<DriverLocationHandler> logger)
        {
            _locationStore = locationStore;
            _logger = logger;
        }
        public Task HandleAsync(DriverLocation location, CancellationToken ct)
        {
            var result = _locationStore.TryApply(location);
            _logger.LogInformation(result.ToString());

            return Task.FromResult(result);
        }
    }
}
