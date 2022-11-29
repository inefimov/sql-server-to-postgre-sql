using System.Globalization;
using Dapper;
using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Models;
using SqlServerToPostgreSql.Services;

namespace SqlServerToPostgreSql.WorkerTasks;

public class CopyRowsTask : IWorkerTask
{
	private readonly ILogger<CopyRowsTask> _logger;
	private readonly WorkerTaskService _service;

	public CopyRowsTask(ILogger<CopyRowsTask> logger, WorkerTaskService service)
	{
		_logger = logger;
		_service = service;
	}

	public async Task Run()
	{
		if (!_service.TasksPlanConfig.CopyRows) return;

		_logger.LogInformation("Trying copy data from tables...");

		var tableNames = await _service.GetTableNames();

		if (!tableNames.Any())
		{
			_logger.LogWarning("Tables not found");
			return;
		}

		foreach (var tableName in tableNames)
		{
			_logger.LogInformation("Table `{TableName}`", tableName);
			await CopyTableRows(tableName);
		}

		_logger.LogInformation("All data copied successfully");
	}

	private async Task CopyTableRows(string tableName)
	{
		var batchSize = _service.SqlServerConfig.BatchSize ?? Props.BatchSize;
		var startOffsetLimit = await GetStartOffsetLimit(tableName);
		var orderBy = await GetOrderBy(tableName);
		var pageIndex = 1;

		while (true)
		{
			var rows = (await GetTableRows(tableName, orderBy, pageIndex++, startOffsetLimit)).ToArray();

			if (startOffsetLimit != null)
			{
				pageIndex = startOffsetLimit.NextPageIndex;
				startOffsetLimit = null;
			}

			await PutTableRows(tableName, rows);

			if (rows.Length < batchSize)
				break;
		}
	}

	private async Task<IEnumerable<IDictionary<string, object?>>> GetTableRows(string tableName, string orderBy, int pageIndex, StartOffsetLimit? startOffsetLimit)
	{
		var batchSize = _service.SqlServerConfig.BatchSize ?? Props.BatchSize;
		var offset = (pageIndex - 1) * batchSize;
		var limit = batchSize;

		if (startOffsetLimit != null)
		{
			offset = startOffsetLimit.Offset;
			limit = startOffsetLimit.Limit;
		}

		var tableSchema = _service.SqlServerConfig.Scheme;
		var tableCatalog = _service.SqlServerConfig.Database;
		var schemeTable = $@"""{tableSchema}"".""{tableName}""";

		_logger.LogDebug(Props.SpaceMs + "Trying get rows starts at {offset} from table `{Schema}.{Catalog}.{Table}`...", offset, tableSchema, tableCatalog, tableName);

		var sql = $@"
select
	t.*
from
	{schemeTable} t
order by
	{orderBy}
offset {offset} rows fetch next {limit} rows only
".Trim();

		using var connection = await _service.SqlServerConnectionFactory.OpenAsync();
		var result = ((await connection.QueryAsync(sql, commandTimeout: Props.CommandTimeout))?.ToArray() ?? Array.Empty<dynamic>())
			.Select(x => (IDictionary<string, object?>)x)
			.ToArray();

		_logger.LogInformation(Props.SpaceMs + "Count received rows from table `{Schema}.{Catalog}.{Table}`: {Count}", tableSchema, tableCatalog, tableName, result.Length);

		return result;
	}

	private async Task PutTableRows(string tableName, IEnumerable<IDictionary<string, object?>> rows)
	{
		rows = rows.ToArray();

		if (!rows.Any()) return;

		var tableSchema = _service.PostgreSqlConfig.Scheme;
		var tableCatalog = _service.PostgreSqlConfig.Database;
		var schemeTable = $@"""{tableSchema}"".""{tableName}""";

		_logger.LogDebug(Props.SpacePg + "Trying put {Count} rows to table `{Schema}.{Catalog}.{Table}`...", rows.Count(), tableSchema, tableCatalog, tableName);

		using var connection = await _service.PostgreSqlConnectionFactory.OpenAsync();

		var pageIndex = 1;

		while (true)
		{
			var batch = rows.Skip((pageIndex - 1) * Props.InsertPageSize).Take(Props.InsertPageSize).ToArray();
			if (!batch.Any()) break;

			var columns = string.Join(", ", batch[0].Keys.Select(x => $@"""{x}"""));
			var values = string.Join(", ", batch.Select(x => "(" + string.Join(", ", x.Values.Select(From)) + ")"));
			var sql = $"insert into {schemeTable} ({columns}) values {values}";

			await connection.ExecuteAsync(sql, commandTimeout: Props.CommandTimeout);

			pageIndex++;
		}

		_logger.LogInformation(Props.SpacePg + "Put {Count} rows to table `{Schema}.{Catalog}.{Table}` completed", rows.Count(), tableSchema, tableCatalog, tableName);
	}

	private async Task<string> GetOrderBy(string tableName)
	{
		var tableSchema = _service.SqlServerConfig.Scheme;
		var tableCatalog = _service.SqlServerConfig.Database;

		_logger.LogDebug(Props.SpaceMs + "Trying create order by for table `{Schema}.{Catalog}.{Table}`...", tableSchema, tableCatalog, tableName);

		var sql = $@"
select
	c.name as 'Name'
from
	sys.tables t, sys.indexes i, sys.index_columns ic, sys.columns c
where
	t.schema_id = schema_id(@tableSchema)
	and t.type = 'U' and t.name = @tableName
	and i.object_id = t.object_id and i.type > 0 and i.is_primary_key = 1
	and ic.object_id = i.object_id and ic.index_id = i.index_id
	and c.object_id = ic.object_id and c.column_id = ic.column_id
order by
	ic.index_column_id
".Trim();

		using var connection = await _service.SqlServerConnectionFactory.OpenAsync();
		var result = (await connection.QueryAsync<string>(sql, new
		{
			tableSchema,
			tableName
		}, commandTimeout: Props.CommandTimeout))?.ToArray() ?? Array.Empty<string>();

		if (!result.Any())
		{
			_logger.LogWarning(Props.SpaceMs + "Primary columns not found for table `{Schema}.{Catalog}.{Table}`. Table will be sorted by first column", tableSchema, tableCatalog, tableName);
			return "1";
		}

		_logger.LogDebug(Props.SpaceMs + "Order by for table `{Schema}.{Catalog}.{Table}` created successfully", tableSchema, tableCatalog, tableName);
		return string.Join(", ", result.Select(x => $@"t.""{x}"""));
	}

	private async Task<StartOffsetLimit?> GetStartOffsetLimit(string tableName)
	{
		var batchSize = _service.SqlServerConfig.BatchSize ?? Props.BatchSize;
		var tableSchema = _service.PostgreSqlConfig.Scheme;
		var tableCatalog = _service.PostgreSqlConfig.Database;
		var schemeTable = $@"""{tableSchema}"".""{tableName}""";

		_logger.LogDebug(Props.SpacePg + "Trying get count rows in table `{Schema}.{Catalog}.{Table}`...", tableSchema, tableCatalog, tableName);

		using var connection = await _service.PostgreSqlConnectionFactory.OpenAsync();
		var offset = await connection.QueryFirstOrDefaultAsync<int>($"select count(1) as c from {schemeTable}", commandTimeout: Props.CommandTimeout);

		if (offset == 0)
		{
			_logger.LogDebug(Props.SpacePg + "Table `{Schema}.{Catalog}.{Table}` is empty", tableSchema, tableCatalog, tableName);
			return null;
		}

		var nextPageIndex = (int)Math.Ceiling(offset / (double)batchSize) + (offset % batchSize == 0 ? 1 : 0);
		var limit = nextPageIndex * batchSize - offset;

		_logger.LogDebug(Props.SpacePg + "Copy rows starts with offset {Offset} and limit {Limit} in table `{Schema}.{Catalog}.{Table}`", offset, limit, tableSchema, tableCatalog, tableName);

		return StartOffsetLimit.From(offset, limit, nextPageIndex);
	}

	private static string From(object? value)
	{
		if (value == null) return "null";
		if (value is bool @bool) return @bool ? "true" : "false";
		if (value is DateTime dateTime) return "'" + dateTime.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture) + "'";
		if (value is float @float) return @float.ToString(CultureInfo.InvariantCulture).Replace(",", ".");
		if (value is decimal @decimal) return @decimal.ToString(CultureInfo.InvariantCulture).Replace(",", ".");
		if (value is double @double) return @double.ToString(CultureInfo.InvariantCulture).Replace(",", ".");
		return "'" + value.ToString()?.Replace("'", "''") + "'";
	}
}
