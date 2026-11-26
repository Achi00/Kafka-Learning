namespace DriverSimulator.Outbox
{
    public class OutboxMessage
    {
        public long Sequence { get; set; }
        public Guid MessageId { get; set; }
        public required string Topic { get; set; }
        public required string MessageKey { get; set; }
        public required string Type { get; set; }
        public required string Payload { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime? ProcessedAt { get; set; }
        public int Attempts { get; set; }
        public string? LastError { get; set; }
    }
}