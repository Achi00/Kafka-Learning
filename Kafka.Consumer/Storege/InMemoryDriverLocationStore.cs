using RideHailing.Contracts.Setup;

namespace LocationService.Storege
{
    public sealed class InMemoryDriverLocationStore : IDriverLocationStore
    {
        // singleton memory store
        private Dictionary<string, DriverLocation> latest = new();

        public ApplyResult TryApply(DriverLocation incoming)
        {
            if (!latest.TryGetValue(incoming.DriverId, out var current) || incoming.Timestamp > current.Timestamp)
            {
                latest[incoming.DriverId] = incoming;
                return ApplyResult.Applied;
            }
            
            if (current.EventId == incoming.EventId)
            {
                return ApplyResult.Duplicate;
            }

            return ApplyResult.Stale;

        }
    }
}
