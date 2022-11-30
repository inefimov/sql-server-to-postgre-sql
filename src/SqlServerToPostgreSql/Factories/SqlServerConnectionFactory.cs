using System.Data;
using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SqlServerToPostgreSql.Configs;

namespace SqlServerToPostgreSql.Factories;

public class SqlServerConnectionFactory : IConnectionFactory
{
	private readonly string _connectionString;
	private readonly SqlServerConfig _sqlServerConfig;

	public SqlServerConnectionFactory(
		IConfiguration configuration,
		SqlServerConfig sqlServerConfig
	)
	{
		_connectionString = configuration.GetConnectionString("SqlServer");
		_sqlServerConfig = sqlServerConfig;
	}

	public async Task<IDbConnection> OpenAsync()
	{
		var result = new SqlConnection(new SqlConnectionStringBuilder(_connectionString)
		{
			InitialCatalog = _sqlServerConfig.Database
		}.ConnectionString);

		await result.OpenAsync();

		return result;
	}
}
