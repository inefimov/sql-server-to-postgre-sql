using System.Data;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace SqlServerToPostgreSql.Factories;

public class PostgreSqlConnectionFactory : IConnectionFactory
{
	private readonly string _connectionString;

	public PostgreSqlConnectionFactory(IConfiguration configuration)
	{
		_connectionString = configuration.GetConnectionString("PostgreSql");
	}

	public async Task<IDbConnection> OpenAsync()
	{
		var result = new NpgsqlConnection(_connectionString);
		await result.OpenAsync();
		return result;
	}
}
