using System.Data;

namespace Kafka.Producer.Database
{
    public interface IDbConnectionFactory
    {
        IDbConnection CreateConnection();
    }
}