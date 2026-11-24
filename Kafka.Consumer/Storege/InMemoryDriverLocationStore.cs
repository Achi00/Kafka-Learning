using Kafka.Shared.Setup;

namespace Kafka.Consumer.Storege
{
    public sealed class InMemoryDriverLocationStore : IDriverLocationStore
    {
        public ApplyResult TryApply(DriverLocation incoming)
        {
            throw new NotImplementedException();
        }
    }
}
