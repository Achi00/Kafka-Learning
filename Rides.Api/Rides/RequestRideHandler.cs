using RideHailing.BuildingBlocks.Data.Interfaces;
using RideHailing.BuildingBlocks.Outbox;
using RideHailing.Contracts.Setup;
using System.Text.Json;

namespace Rides.Api.Rides
{
    public sealed class RequestRideHandler
    {
        private readonly IUnitOfWork _unitOfWork;
        private readonly RideRepository _rideRepository;
        private readonly OutboxRepository _outboxRepository;

        public RequestRideHandler(IUnitOfWork unitOfWork, RideRepository rideRepository, OutboxRepository outboxRepository)
        {
            _outboxRepository = outboxRepository;
            _rideRepository = rideRepository;
            _unitOfWork = unitOfWork;
        }

        public async Task RequestRideAsync(Ride ride, CancellationToken ct)
        {
            await _unitOfWork.BeginAsync(ct);

            await _rideRepository.CreateRide(ride);

            // event model wich carries ride entity
            var evt = new RideRequested(Guid.NewGuid(), ride.Id, ride.RiderId, ride.PickupLat, ride.PickupLng, DateTime.UtcNow);

            await _outboxRepository.AddAsync(new OutboxMessage
            {
                MessageId = evt.EventId,
                Topic = Topics.RideEvents,
                MessageKey = ride.Id.ToString(),
                Type = nameof(RideRequested),
                Payload = JsonSerializer.Serialize(evt)
            });

            await _unitOfWork.CommitAsync(ct);
        }
    }
}
