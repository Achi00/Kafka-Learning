using RideHailing.BuildingBlocks.Data.Interfaces;
using RideHailing.BuildingBlocks.Outbox;
using RideHailing.Contracts.Setup;
using Rides.Api.Rides.Enums;
using Rides.Api.Rides.Models;
using Rides.Api.Rides.Repositories;
using System.Text.Json;

namespace Rides.Api.Rides
{
    public sealed class RequestRideHandler
    {
        private readonly IUnitOfWork _unitOfWork;
        private readonly RideRepository _rideRepository;
        private readonly OutboxRepository _outboxRepository;
        private readonly TimeProvider _time;

        public RequestRideHandler(IUnitOfWork unitOfWork, RideRepository rideRepository, OutboxRepository outboxRepository, TimeProvider time)
        {
            _outboxRepository = outboxRepository;
            _rideRepository = rideRepository;
            _unitOfWork = unitOfWork;
            _time = time;
        }

        public async Task<Guid> HandleAsync(RequestRide command, CancellationToken ct)
        {
            var now = DateTime.UtcNow;

            var ride = new Ride
            {
                Id = Guid.NewGuid(),
                RiderId = command.RiderId,
                PickupLat = command.PickupLat,
                PickupLng = command.PickupLng,
                Status = RideStatus.Requested,
                RequestedAt = now
            };

            // event model wich carries ride entity
            var evt = new RideRequested(
                EventId: Guid.NewGuid(),
                RideId: ride.Id,
                RiderId: ride.RiderId,
                PickupLat: ride.PickupLat,
                PickupLng: ride.PickupLng,
                RequestedAt: new DateTimeOffset(now));

            // whole operation should be single transaction
            await _unitOfWork.BeginAsync(ct);

            await _rideRepository.CreateRide(ride);

            await _outboxRepository.AddAsync(new OutboxMessage
            {
                MessageId = evt.EventId,
                Topic = Topics.RideEvents,
                MessageKey = ride.Id.ToString(),
                Type = nameof(RideRequested),
                Payload = JsonSerializer.Serialize(evt)
            });

            await _unitOfWork.CommitAsync(ct);

            return ride.Id;
        }
    }
}
