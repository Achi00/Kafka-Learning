using Microsoft.Extensions.DependencyInjection;

namespace RideHailing.Contracts
{
    public static class DependencyInjection
    {
        public static IServiceCollection AddKafka(this IServiceCollection services)
        {
            return services;
        }
    }
}
