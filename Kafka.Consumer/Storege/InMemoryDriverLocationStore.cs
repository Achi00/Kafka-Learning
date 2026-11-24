using Kafka.Shared.Setup;

namespace Kafka.Consumer.Storege
{
    public sealed class InMemoryDriverLocationStore : IDriverLocationStore
    {
        private static Dictionary<string, DriverLocation> latest = new Dictionary<string, DriverLocation>();

        public ApplyResult TryApply(DriverLocation incoming)
        {
            if (!latest.TryGetValue(incoming.DriverId, out var value))
            {
                latest[incoming.DriverId] = incoming;
                return ApplyResult.Applied;
            }
            else if (value.EventId == incoming.EventId)
            {
                return ApplyResult.Duplicate;
            }
            else
            {
                return ApplyResult.Stale;
            }
        }
    }
}
