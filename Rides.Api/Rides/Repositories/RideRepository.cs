using Dapper;
using RideHailing.BuildingBlocks.Data.Interfaces;
using Rides.Api.Rides.Models;

namespace Rides.Api.Rides.Repositories
{
    public class RideRepository
    {
        private readonly IUnitOfWork _unitOfWork;

        public RideRepository(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public Task CreateRide(Ride ride)
        {
            const string sql = """
                INSERT INTO dbo.OutboxMessages (RiderId, PickupLat, PickupLng, Status, RequestedAt)
                VALUES (@RiderId, @PickupLat, @PickupLng, @Status, @RequestedAt)
             """;

            var data = new
            {
                ride.Id,
                ride.RiderId,
                ride.PickupLat,
                ride.PickupLng,
                Status = ride.Status.ToString(),
                ride.RequestedAt
            };

            return _unitOfWork.Connection.ExecuteAsync(
                sql, 
                data, 
                _unitOfWork.Transaction);
        }
    }
}
