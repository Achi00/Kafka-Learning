using System.ComponentModel.DataAnnotations;

namespace LocationService
{
    public sealed class DriverLocationConsumerOptions
    {
        public const string Section = "Kafka:Consumer";
        [Required] public string GroupId { get; init; } = "";
    }
}
