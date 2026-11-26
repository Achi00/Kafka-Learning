using Microsoft.Data.SqlClient;
using RideHailing.BuildingBlocks.Interfaces;
using System.Data.Common;

namespace RideHailing.BuildingBlocks
{
    public sealed class SqlConnectionFactory : IDbConnectionFactory
    {
        private readonly string _connectionString;

        public SqlConnectionFactory(string connectionString)
        {
            _connectionString = connectionString;
        }

        public DbConnection CreateConnection()
        {
            return new SqlConnection(_connectionString);
        }
    }
}
