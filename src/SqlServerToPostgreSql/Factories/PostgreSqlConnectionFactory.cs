using System.Data;
using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using SqlServerToPostgreSql.Configs;

namespace SqlServerToPostgreSql.Factories;

public class PostgreSqlConnectionFactory : IConnectionFactory
{
	private readonly string _connectionString;
	private readonly PostgreSqlConfig _postgreSqlConfig;

	public PostgreSqlConnectionFactory(
		IConfiguration configuration,
		PostgreSqlConfig postgreSqlConfig
	)
	{
		_connectionString = configuration.GetConnectionString("PostgreSql");
		_postgreSqlConfig = postgreSqlConfig;
	}

	public async Task<IDbConnection> OpenAsync()
	{
		var result = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(_connectionString)
		{
			Database = null
		}.ConnectionString);

		await result.OpenAsync();
		await ChangeDatabase(result);

		return result;
	}

	private async Task ChangeDatabase(NpgsqlConnection connection)
	{
		var database = _postgreSqlConfig.Database;

		var exists = await connection.QueryFirstOrDefaultAsync<int>("select 1 as c from pg_database where datname = @database", new { database }) > 0;

		if (!exists)
			await connection.ExecuteAsync($@"create database ""{database}"";");

		await connection.ChangeDatabaseAsync(database);
	}
}
