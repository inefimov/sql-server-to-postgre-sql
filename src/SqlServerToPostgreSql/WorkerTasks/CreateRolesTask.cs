using Dapper;
using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Services;

namespace SqlServerToPostgreSql.WorkerTasks;

public class CreateRolesTask : IWorkerTask
{
	private readonly ILogger<CreateRolesTask> _logger;
	private readonly WorkerTaskService _service;

	public CreateRolesTask(ILogger<CreateRolesTask> logger, WorkerTaskService service)
	{
		_logger = logger;
		_service = service;
	}

	public async Task Run()
	{
		if (!_service.TasksPlanConfig.CreateRoles) return;

		var roles = _service.PostgreSqlConfig.Roles;

		if (roles?.Any() != true) return;

		var scheme = _service.PostgreSqlConfig.Scheme;
		var database = _service.PostgreSqlConfig.Database;

		foreach (var role in roles)
		{
			var name = role.Key;
			var with = string.IsNullOrEmpty(role.Value) ? "" : $"with login encrypted password '{role.Value.Replace("'", "''")}'";

			_logger.LogDebug("Trying create role `{Name}`...", name);

			using var connection = await _service.PostgreSqlConnectionFactory.OpenAsync();

			var exists = await connection.QueryFirstOrDefaultAsync<int>("select 1 as c from pg_roles where rolname = @name", new { name }) > 0;
			var sql = $@"
{(exists ? "alter" : "create")} role ""{name}"" {with};
grant all privileges on all tables in schema ""{scheme}"" to ""{name}"";
grant all privileges on schema ""{scheme}"" to ""{name}"";
grant all privileges on database ""{database}"" to ""{name}"";
".Trim();

			await connection.ExecuteAsync(sql, commandTimeout: Props.CommandTimeout);

			_logger.LogInformation("Role `{Name}` {Action} successfully", name, exists ? "updated" : "created");
		}
	}
}
