using Dapper;
using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Models;
using SqlServerToPostgreSql.Services;

namespace SqlServerToPostgreSql.WorkerTasks;

public abstract class CreateTableConstraintsTask : IWorkerTask
{
	private readonly string _constraintType;
	private readonly ILogger<CreateTableConstraintsTask> _logger;
	private readonly WorkerTaskService _service;

	protected CreateTableConstraintsTask(string constraintType, ILogger<CreateTableConstraintsTask> logger, WorkerTaskService service)
	{
		_constraintType = constraintType;
		_logger = logger;
		_service = service;
	}

	public async Task Run()
	{
		if (!_service.TasksPlanConfig.CreateTableConstraints) return;

		_logger.LogInformation("Trying create `{Keys}`...", _constraintType);

		var tableNames = await _service.GetTableNames();

		if (!tableNames.Any())
		{
			_logger.LogWarning("Tables not found");
			return;
		}

		foreach (var tableName in tableNames)
		{
			_logger.LogInformation("Table `{TableName}`", tableName);
			await CreateTableConstraints(tableName);
		}

		_logger.LogInformation("All `{Keys}` created successfully", _constraintType);
	}

	private async Task CreateTableConstraints(string tableName)
	{
		var tableSchema = _service.PostgreSqlConfig.Scheme;
		var tableCatalog = _service.PostgreSqlConfig.Database;

		_logger.LogDebug(Props.SpacePg + "Trying create `{Keys}` for table `{Schema}.{Catalog}.{Table}`...", _constraintType, tableSchema, tableCatalog, tableName);

		var constraints = (await GetTableConstraints(tableName)).ToArray();

		if (!constraints.Any())
		{
			_logger.LogDebug(Props.SpacePg + "Not found `{Keys}` in table `{Schema}.{Catalog}.{Table}`...", _constraintType, tableSchema, tableCatalog, tableName);
			return;
		}

		var sql = string.Join(";\n", constraints.Select(x => x.ToSql(tableSchema, tableName)));

		using var connection = await _service.PostgreSqlConnectionFactory.OpenAsync();
		await connection.ExecuteAsync(sql, commandTimeout: Props.CommandTimeout);

		_logger.LogInformation(Props.SpacePg + "All `{Keys}` for table `{Schema}.{Catalog}.{Table}` created successfully. Count: {Count}", _constraintType, tableSchema, tableCatalog, tableName, constraints.Length);
	}

	private async Task<IEnumerable<SqlServerTableConstraint>> GetTableConstraints(string tableName)
	{
		var tableSchema = _service.SqlServerConfig.Scheme;
		var tableCatalog = _service.SqlServerConfig.Database;

		_logger.LogDebug(Props.SpaceMs + "Trying get `{Keys}` for table `{Schema}.{Catalog}.{Table}`...", _constraintType, tableSchema, tableCatalog, tableName);

		var sql = TableConstraintQuery;

		using var connection = await _service.SqlServerConnectionFactory.OpenAsync();
		var result = (await connection.QueryAsync<SqlServerTableConstraint>(sql, new
		{
			tableCatalog,
			tableSchema,
			constraintType = _constraintType,
			tableName
		}, commandTimeout: Props.CommandTimeout))?.ToArray() ?? Array.Empty<SqlServerTableConstraint>();

		_logger.LogDebug(Props.SpaceMs + "Count of `{Keys}` found for table `{Schema}.{Catalog}.{Table}`: {Count}", _constraintType, tableSchema, tableCatalog, tableName, result.Length);

		return result;
	}

	protected virtual string TableConstraintQuery => @"
select
	t.Name, t.Type, right(t.Columns, len(t.Columns) - 1) as Columns
from (
	select
		t.CONSTRAINT_NAME as 'Name',
		t.CONSTRAINT_TYPE as 'Type',
		(
			select
				',' + c.COLUMN_NAME
			from
				INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
			where
				c.CONSTRAINT_CATALOG = t.CONSTRAINT_CATALOG and c.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA and c.CONSTRAINT_NAME = t.CONSTRAINT_NAME and c.TABLE_NAME = t.TABLE_NAME
			order by
				c.ORDINAL_POSITION
			for xml path ('')
		) as Columns
	from
		INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
	where
		t.CONSTRAINT_CATALOG = @tableCatalog
		and t.CONSTRAINT_SCHEMA = @tableSchema
		and t.CONSTRAINT_TYPE = @constraintType
		and t.TABLE_NAME = @tableName
) t
".Trim();
}
