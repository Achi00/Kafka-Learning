using Kafka.Shared.Setup;

await KafkaConfig.EnsureTopicAsync("localhost:9092", "Topics.DriverLocations", 3);