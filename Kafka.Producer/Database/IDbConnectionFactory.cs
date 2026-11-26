using System.Data.Common;

namespace Kafka.Producer.Database
{
    public interface IDbConnectionFactory
    {
        DbConnection CreateConnection();
    }
}