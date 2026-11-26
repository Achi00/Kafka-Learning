namespace Rides.Api.Rides
{
    public sealed record RequestRide(string RiderId, double PickupLat, double PickupLng);
}