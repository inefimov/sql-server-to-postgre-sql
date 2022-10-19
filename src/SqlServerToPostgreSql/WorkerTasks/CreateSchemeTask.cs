using Dapper;
using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Services;

namespace SqlServerToPostgreSql.WorkerTasks;

public class CreateSchemeTask : IWorkerTask
{
	private readonly ILogger<CreateSchemeTask> _logger;
	private readonly WorkerTaskService _service;

	public CreateSchemeTask(ILogger<CreateSchemeTask> logger, WorkerTaskService service)
	{
		_logger = logger;
		_service = service;
	}

	public async Task Run()
	{
		if (!_service.TasksPlanConfig.CreateScheme) return;

		var scheme = _service.PostgreSqlConfig.Scheme;
		var owner = _service.PostgreSqlConfig.SuperUser;

		_logger.LogDebug("Trying create scheme `{Name}` for owner `{Owner}`...", scheme, owner);

		var sql = $@"
create schema if not exists ""{scheme}"";
alter schema ""{scheme}"" owner to {owner};
".Trim();

		using var connection = await _service.PostgreSqlConnectionFactory.OpenAsync();
		await connection.ExecuteAsync(sql, commandTimeout: Props.CommandTimeout);

		_logger.LogInformation("Scheme `{Name}` created successfully for owner `{Owner}`", scheme, owner);
	}
}
