using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace RideHailing.Contracts.Setup
{
    public static class KafkaConfig
    {
        public static async Task EnsureTopicAsync(string bootstrap, string name, int partitions)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrap,
            }).Build();

            try
            {
                await admin.CreateTopicsAsync(new[]
                {
                    new TopicSpecification { Name = name, NumPartitions = partitions, ReplicationFactor = 1 }
                });
            }
            catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            {
                // already exists ignore
            }
        }
    }
}
