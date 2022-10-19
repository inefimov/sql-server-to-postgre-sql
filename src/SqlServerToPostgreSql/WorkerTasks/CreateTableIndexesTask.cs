using Dapper;
using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Models;
using SqlServerToPostgreSql.Services;

namespace SqlServerToPostgreSql.WorkerTasks;

public class CreateTableIndexesTask : IWorkerTask
{
	private readonly ILogger<CreateTableIndexesTask> _logger;
	private readonly WorkerTaskService _service;

	public CreateTableIndexesTask(ILogger<CreateTableIndexesTask> logger, WorkerTaskService service)
	{
		_logger = logger;
		_service = service;
	}

	public async Task Run()
	{
		if (!_service.TasksPlanConfig.CreateTableIndexes) return;

		_logger.LogDebug("Trying create indexes for tables...");

		var tableNames = await _service.GetTableNames();

		if (!tableNames.Any())
		{
			_logger.LogWarning("Indexes not found");
			return;
		}

		foreach (var tableName in tableNames)
		{
			_logger.LogInformation("Table `{TableName}`", tableName);
			await CreateTableIndexes(tableName);
		}

		_logger.LogInformation("All indexes for tables created successfully");
	}

	private async Task CreateTableIndexes(string tableName)
	{
		var tableSchema = _service.PostgreSqlConfig.Scheme;
		var tableCatalog = _service.PostgreSqlConfig.Database;

		_logger.LogDebug(Props.SpacePg + "Trying create indexes for table `{Schema}.{Catalog}.{Table}`...", tableSchema, tableCatalog, tableName);

		var indexes = (await GetTableIndexes(tableName)).ToArray();

		if (!indexes.Any())
		{
			_logger.LogDebug(Props.SpacePg + "Not found indexes in table `{Schema}.{Catalog}.{Table}`...", tableSchema, tableCatalog, tableName);
			return;
		}

		var sql = string.Join(";\n", indexes.Select(x => x.ToSql(tableSchema, tableName)));

		using var connection = await _service.PostgreSqlConnectionFactory.OpenAsync();
		await connection.ExecuteAsync(sql, commandTimeout: Props.CommandTimeout);

		_logger.LogInformation(Props.SpacePg + "All indexes for table `{Schema}.{Catalog}.{Table}` created successfully. Count: {Count}", tableSchema, tableCatalog, tableName, indexes.Length);
	}

	private async Task<IEnumerable<SqlServerTableIndex>> GetTableIndexes(string tableName)
	{
		var tableSchema = _service.SqlServerConfig.Scheme;
		var tableCatalog = _service.SqlServerConfig.Database;

		_logger.LogDebug(Props.SpaceMs + "Trying get indexes for table `{Schema}.{Catalog}.{Table}`...", tableSchema, tableCatalog, tableName);

		var sql = @"
select
	t.Name, t.IsUnique,
	right(t.Columns, len(t.Columns) - 1) as Columns,
	right(t.IncludedColumns, len(t.IncludedColumns) - 1) as IncludedColumns
from (
	select
		i.name as 'Name',
		cast(i.is_unique as bit) as IsUnique,
		(
			select
				',""' + c.name + '""' + iif(ic.is_descending_key = 1, ' desc', '')
			from
				sys.index_columns ic, sys.columns c
			where
				ic.object_id = i.object_id and ic.index_id = i.index_id and ic.is_included_column = 0
				and c.object_id = ic.object_id and c.column_id = ic.column_id
			order by
				ic.index_column_id
			for xml path ('')
		) as Columns,
		(
			select
				',""' + c.name + '""' + iif(ic.is_descending_key = 1, ' desc', '')
			from
				sys.index_columns ic, sys.columns c
			where
				ic.object_id = i.object_id and ic.index_id = i.index_id and ic.is_included_column = 1
				and c.object_id = ic.object_id and c.column_id = ic.column_id
			order by
				ic.index_column_id
			for xml path ('')
		) as IncludedColumns
	from
		sys.tables t, sys.indexes i
	where
		t.schema_id = schema_id(@tableSchema)
		and t.type = 'U' and t.name = @tableName
		and i.object_id = t.object_id and i.type > 0 and i.is_primary_key = 0 and i.is_unique_constraint = 0
) t
".Trim();

		using var connection = await _service.SqlServerConnectionFactory.OpenAsync();
		var result = (await connection.QueryAsync<SqlServerTableIndex>(sql, new
		{
			tableSchema,
			tableName
		}, commandTimeout: Props.CommandTimeout))?.ToArray() ?? Array.Empty<SqlServerTableIndex>();

		_logger.LogDebug(Props.SpaceMs + "Count of indexes found for table `{Schema}.{Catalog}.{Table}`: {Count}", tableSchema, tableCatalog, tableName, result.Length);

		return result;
	}
}
