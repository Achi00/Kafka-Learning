namespace Rides.Api.Rides
{
    public sealed record RideRequested(Guid EventId, Guid RideId, string RiderId, double PickupLat, double PickupLng, DateTimeOffset CreatedAt);
}
