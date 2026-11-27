using Microsoft.AspNetCore.Http.HttpResults;
using Rides.Api.Rides.Models;
using Rides.Api.Rides.Models.DTOs;

namespace Rides.Api.Rides.Endpoints
{
    public static class RideEndpoints
    {
        public static IEndpointRouteBuilder MapRideEndpoints(this IEndpointRouteBuilder app)
        {
            var rides = app.MapGroup("/rides").WithTags("Rides");

            rides.MapPost("/", RequestRide).WithName("RequestRide");

            return app;
        }

        private static async Task<Created<RideCreatedResponse>> RequestRide(
        RequestRide command,
        RequestRideHandler handler,
        CancellationToken ct)
        {
            var rideId = await handler.HandleAsync(command, ct);
            return TypedResults.Created($"/rides/{rideId}", new RideCreatedResponse(rideId));
        }
    }
}
