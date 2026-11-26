namespace RideHailing.Contracts.Setup
{
    public record DriverLocation(Guid EventId, string DriverId, double Lat, double Lng, DateTimeOffset Timestamp);
}
