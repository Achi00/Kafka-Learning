namespace Rides.Api.Rides.Models
{
    public sealed record RequestRide(string RiderId, double PickupLat, double PickupLng);
}