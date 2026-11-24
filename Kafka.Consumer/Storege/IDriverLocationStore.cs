using Kafka.Shared.Setup;

namespace Kafka.Consumer
{
    public interface IDriverLocationStore
    {
        ApplyResult TryApply(DriverLocation incoming);
    }

    // state of message type: DriverLocation
    public enum ApplyResult { Applied, Duplicate, Stale }
}
