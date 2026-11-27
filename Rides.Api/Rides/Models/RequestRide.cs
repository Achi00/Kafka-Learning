using System.ComponentModel.DataAnnotations;

namespace Rides.Api.Rides.Models
{
    public sealed record RequestRide(
        [Required, StringLength(50)] string RiderId,
        [Range(41.6, 41.85)] double PickupLat,
        [Range(44.6, 45.0)] double PickupLng);
}