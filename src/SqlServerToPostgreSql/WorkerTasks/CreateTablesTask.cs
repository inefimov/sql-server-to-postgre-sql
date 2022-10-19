using Dapper;
using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Models;
using SqlServerToPostgreSql.Services;

namespace SqlServerToPostgreSql.WorkerTasks;

public class CreateTablesTask : IWorkerTask
{
	private readonly ILogger<CreateTablesTask> _logger;
	private readonly WorkerTaskService _service;

	public CreateTablesTask(ILogger<CreateTablesTask> logger, WorkerTaskService service)
	{
		_logger = logger;
		_service = service;
	}

	public async Task Run()
	{
		if (!_service.TasksPlanConfig.CreateTables) return;

		_logger.LogDebug("Trying create tables...");

		var tableNames = await _service.GetTableNames();

		if (!tableNames.Any())
		{
			_logger.LogWarning("Tables not found");
			return;
		}

		foreach (var tableName in tableNames)
		{
			_logger.LogInformation("Table `{TableName}`", tableName);
			await CreateTable(tableName);
		}

		_logger.LogInformation("All tables created successfully");
	}

	private async Task CreateTable(string tableName)
	{
		var tableSchema = _service.PostgreSqlConfig.Scheme;
		var tableCatalog = _service.PostgreSqlConfig.Database;

		_logger.LogDebug(Props.SpacePg + "Trying create table `{Table}` in `{Schema}.{Catalog}`...", tableName, tableSchema, tableCatalog);

		var schemeTable = $@"""{tableSchema}"".""{tableName}""";
		var tableColumns = "(" + string.Join(", ", (await GetTableColumns(tableName)).Select(x => x.ToSql())) + ")";

		var sql = $@"
drop table if exists {schemeTable} cascade;
create table {schemeTable} {tableColumns};
".Trim();

		using var connection = await _service.PostgreSqlConnectionFactory.OpenAsync();
		await connection.ExecuteAsync(sql, commandTimeout: Props.CommandTimeout);

		_logger.LogInformation(Props.SpacePg + "Table `{Table}` in `{Schema}.{Catalog}` created successfully", tableName, tableSchema, tableCatalog);
	}

	private async Task<IEnumerable<SqlServerTableColumn>> GetTableColumns(string tableName)
	{
		var tableSchema = _service.SqlServerConfig.Scheme;
		var tableCatalog = _service.SqlServerConfig.Database;

		_logger.LogDebug(Props.SpaceMs + "Trying get columns of table `{Table}` in `{Schema}.{Catalog}`...", tableName, tableSchema, tableCatalog);

		var sql = @"
select
	t.COLUMN_NAME as 'Name',
	t.COLUMN_DEFAULT as 'Default',
	cast((IIF(UPPER(t.IS_NULLABLE) = 'YES', 1, 0)) as bit) as IsNullable,
	t.DATA_TYPE as DataType,
	t.CHARACTER_MAXIMUM_LENGTH as 'MaxLength',
	t.NUMERIC_PRECISION as NumericPrecision,
	t.NUMERIC_SCALE as NumericScale,
	cast(IIF(sc.column_id is null, 0, 1) as bit) as IsIdentity,
	(cast(sc.last_value as int) + 1) as IdentitySeed,
	sc.increment_value as IdentityIncrement
from
	INFORMATION_SCHEMA.COLUMNS t
left join
	sys.identity_columns sc
on
	sc.object_id = OBJECT_ID(t.TABLE_NAME)
	and sc.column_id = COLUMNPROPERTY(OBJECT_ID(t.TABLE_NAME), t.COLUMN_NAME, 'ColumnId')
where
	t.TABLE_CATALOG = @tableCatalog
	and t.TABLE_SCHEMA = @tableSchema
	and t.TABLE_NAME = @tableName
order by
	t.TABLE_NAME, t.ORDINAL_POSITION
".Trim();

		using var connection = await _service.SqlServerConnectionFactory.OpenAsync();
		var result = (await connection.QueryAsync<SqlServerTableColumn>(sql, new
		{
			tableCatalog,
			tableSchema,
			tableName
		}, commandTimeout: Props.CommandTimeout))?.ToArray() ?? Array.Empty<SqlServerTableColumn>();

		_logger.LogDebug(Props.SpaceMs + "Count of columns found in `{Table}` in `{Schema}.{Catalog}`: {Count}", tableName, tableSchema, tableCatalog, result.Length);

		return result;
	}
}
