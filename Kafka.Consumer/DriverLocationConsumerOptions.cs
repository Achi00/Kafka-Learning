using System.ComponentModel.DataAnnotations;

namespace Kafka.Consumer
{
    public sealed class DriverLocationConsumerOptions
    {
        public const string Section = "Kafka:Consumer";
        [Required] public string GroupId { get; init; } = "";
    }
}
