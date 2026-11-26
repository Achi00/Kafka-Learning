namespace RideHailing.Contracts.Setup
{
    public static class Topics
    {
        public const string DriverLocations = "driver-locations";
        public const int DriverLocationsPartitions = 3;

        // for group based dlq and retry, avoids shared topics for corrupt or missleading results
        public static string DeadLetter(string groupId, string topic) => $"{groupId}.{topic}.dlq";
        public static string Retry(string groupId, string topic) => $"{groupId}.{topic}.retry";
    }
}
