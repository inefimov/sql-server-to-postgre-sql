using Dapper;
using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Configs;
using SqlServerToPostgreSql.Factories;

namespace SqlServerToPostgreSql.Services;

public class WorkerTaskService
{
	private readonly ILogger<WorkerTaskService> _logger;

	public TasksPlanConfig TasksPlanConfig { get; }
	public SqlServerConfig SqlServerConfig { get; }
	public PostgreSqlConfig PostgreSqlConfig { get; }
	public SqlServerConnectionFactory SqlServerConnectionFactory { get; }
	public PostgreSqlConnectionFactory PostgreSqlConnectionFactory { get; }

	public WorkerTaskService(
		ILogger<WorkerTaskService> logger,
		TasksPlanConfig tasksPlanConfig,
		SqlServerConfig sqlServerConfig,
		PostgreSqlConfig postgreSqlConfig,
		SqlServerConnectionFactory sqlServerConnectionFactory,
		PostgreSqlConnectionFactory postgreSqlConnectionFactory)
	{
		_logger = logger;
		TasksPlanConfig = tasksPlanConfig;
		SqlServerConfig = sqlServerConfig;
		PostgreSqlConfig = postgreSqlConfig;
		SqlServerConnectionFactory = sqlServerConnectionFactory;
		PostgreSqlConnectionFactory = postgreSqlConnectionFactory;
	}

	public async Task<string[]> GetTableNames()
	{
		var tableSchema = SqlServerConfig.Scheme;
		var tableCatalog = SqlServerConfig.Database;

		_logger.LogDebug("Trying get table names in `{Schema}.{Catalog}`...", tableSchema, tableCatalog);

		var sql = @"
select
	t.TABLE_NAME
from
	INFORMATION_SCHEMA.TABLES t
where
	t.TABLE_CATALOG = @tableCatalog
	and t.TABLE_SCHEMA = @tableSchema
	and t.TABLE_TYPE = 'BASE TABLE'
order by
	t.TABLE_NAME
".Trim();

		using var connection = await SqlServerConnectionFactory.OpenAsync();
		var result = (await connection.QueryAsync<string>(sql, new
		{
			tableCatalog,
			tableSchema
		}, commandTimeout: Props.CommandTimeout))?.ToArray() ?? Array.Empty<string>();

		var includeTables = PostgreSqlConfig.IncludeTables?.ToArray() ?? Array.Empty<string>();
		var excludeTables = PostgreSqlConfig.ExcludeTables?.ToArray() ?? Array.Empty<string>();

		if (includeTables.Any())
			result = result.Where(x => includeTables.Contains(x, StringComparer.OrdinalIgnoreCase)).ToArray();

		if (excludeTables.Any())
			result = result.Where(x => !excludeTables.Contains(x, StringComparer.OrdinalIgnoreCase)).ToArray();

		_logger.LogDebug("Count of tables found in `{Schema}.{Catalog}`: {Count}", tableSchema, tableCatalog, result.Length);

		return result;
	}
}
