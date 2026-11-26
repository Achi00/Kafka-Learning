using System.Data.Common;

namespace RideHailing.BuildingBlocks.Interfaces
{
    public interface IDbConnectionFactory
    {
        DbConnection CreateConnection();
    }
}