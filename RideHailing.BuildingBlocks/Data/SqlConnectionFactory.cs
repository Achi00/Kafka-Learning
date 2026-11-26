using Microsoft.Data.SqlClient;
using RideHailing.BuildingBlocks.Data.Interfaces;
using System.Data.Common;

namespace RideHailing.BuildingBlocks.Data
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
