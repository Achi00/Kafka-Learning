namespace Rides.Api.Rides.Models
{
    public sealed record RideRequested(Guid EventId, Guid RideId, string RiderId, double PickupLat, double PickupLng, DateTimeOffset RequestedAt);
}
