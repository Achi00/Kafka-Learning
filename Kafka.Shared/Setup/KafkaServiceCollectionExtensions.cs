using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;


namespace RideHailing.Contracts.Setup
{
    public static class KafkaServiceCollectionExtensions
    {
        public static IServiceCollection AddKafka(this IServiceCollection services)
        {
            services.AddOptions<KafkaOptions>()
                .BindConfiguration(KafkaOptions.Section)
                .ValidateDataAnnotations()
                .ValidateOnStart();

            services.AddSingleton<IProducer<string, string>>(sp =>
            {
                var o = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
                return new ProducerBuilder<string, string>(new ProducerConfig
                {
                    BootstrapServers = o.BootstrapServers,
                    Acks = Acks.All,
                    EnableIdempotence = true
                }).Build();
            });

            return services;
        }
    }
}
