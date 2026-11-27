using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Rides.Api.Rides;

namespace Rides.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class RideController : ControllerBase
    {
        public async Task<IResult> CreateRide(RequestRide command, RequestRideHandler handler, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(command.RiderId))
            {
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["riderId"] = ["RiderId is required."]
                });
            }
            if (command.PickupLat is < 41.6 or > 41.85 || command.PickupLng is < 44.6 or > 45.0)
            {
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["pickup"] = ["Pickup must be inside Tbilisi."]
                });
            }

            var rideId = await handler.HandleAsync(command, ct);

            return Results.Created($"/rides/{rideId}", new { rideId });
        }
    }
}
