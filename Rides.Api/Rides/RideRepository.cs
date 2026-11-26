using Dapper;
using RideHailing.BuildingBlocks.Data.Interfaces;

namespace Rides.Api.Rides
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
                INSERT INTO dbo.OutboxMessages (RiderId, PickupLat, PickupLng, Status, CreatedAt)
                VALUES (@RiderId, @PickupLat, @PickupLng, @Status, @CreatedAt)
             """;

            return _unitOfWork.Connection.ExecuteAsync(sql, ride, _unitOfWork.Transaction);
        }
    }
}
