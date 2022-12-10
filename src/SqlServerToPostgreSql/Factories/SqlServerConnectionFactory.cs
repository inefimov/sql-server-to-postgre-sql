using System.Data;
using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SqlServerToPostgreSql.Configs;

namespace SqlServerToPostgreSql.Factories;

public class SqlServerConnectionFactory : IConnectionFactory
{
	private readonly string _connectionString;

	public SqlServerConnectionFactory(
		IConfiguration configuration
	)
	{
		_connectionString = configuration.GetConnectionString("SqlServer");
	}

	public async Task<IDbConnection> OpenAsync()
	{
		var result = new SqlConnection(_connectionString);

		await result.OpenAsync();

		return result;
	}
}
