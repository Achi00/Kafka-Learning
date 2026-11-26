using System.ComponentModel.DataAnnotations;

namespace RideHailing.Contracts.Setup
{
    public sealed class KafkaOptions
    {
        public const string Section = "Kafka";
        [Required] public string BootstrapServers { get; init; } = "";
    }
}
