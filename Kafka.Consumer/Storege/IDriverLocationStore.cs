using RideHailing.Contracts.Setup;

namespace LocationService.Storege
{
    public interface IDriverLocationStore
    {
        ApplyResult TryApply(DriverLocation incoming);
    }

    // state of message type: DriverLocation
    public enum ApplyResult { Applied, Duplicate, Stale }
}
