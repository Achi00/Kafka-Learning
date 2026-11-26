namespace Kafka.Producer.Database
{
    public class OutboxMessage
    {
        public long Sequence { get; set; }
        public Guid MessageId { get; set; }
        public string Topic { get; set; }
        public string MessageKey { get; set; }
        public string Type { get; set; }
        public string Payload { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ProcessedAt { get; set; }
        public DateTimeOffset Attempts { get; set; }
        public string LastError { get; set; }
    }
}