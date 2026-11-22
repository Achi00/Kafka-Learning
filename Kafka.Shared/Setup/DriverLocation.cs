namespace Kafka.Shared.Setup
{
    public record DriverLocation(Guid EventId, string DriverId, double Lat, double Lng, DateTimeOffset Timestamp);
}
