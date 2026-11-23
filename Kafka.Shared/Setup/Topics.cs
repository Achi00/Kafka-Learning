namespace Kafka.Shared.Setup
{
    public static class Topics
    {
        public const string DriverLocations = "driver-locations";
        public const int DriverLocationsPartitions = 3;
        // dead letter queue for failed messages
        public const string DriverLocationsDlq = "driver-locations.dlq";
    }
}
