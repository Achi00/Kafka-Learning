using System.Data.Common;

namespace RideHailing.BuildingBlocks.Data.Interfaces
{
    public interface IDbConnectionFactory
    {
        DbConnection CreateConnection();
    }
}