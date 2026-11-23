using System.ComponentModel.DataAnnotations;

namespace Kafka.Shared.Setup
{
    public sealed class KafkaOptions
    {
        public const string Section = "Kafka";

        [Required]
        public string BootstrapServers { get; init; }
        
        [Required]
        public string GroupId { get; init; } = "";

    }
}
