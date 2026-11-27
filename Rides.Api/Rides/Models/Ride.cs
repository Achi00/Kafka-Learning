using Rides.Api.Rides.Enums;

namespace Rides.Api.Rides.Models
{
    public sealed class Ride
    {
        public Guid Id { get; init; }
        public required string RiderId { get; init; }
        public double PickupLat { get; init; }
        public double PickupLng { get; init; }
        public required RideStatus Status { get; init; }
        public DateTime RequestedAt { get; init; }
    }
}
