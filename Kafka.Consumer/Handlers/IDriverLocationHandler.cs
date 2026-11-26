using RideHailing.Contracts.Setup;

namespace LocationService.Handlers
{
    public interface IDriverLocationHandler
    {
        Task HandleAsync(DriverLocation location, CancellationToken ct);
    }
}