using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Factories;

namespace SqlServerToPostgreSql.WorkerTasks;

public class TestConnectionTask : IWorkerTask
{
	private readonly ILogger<TestConnectionTask> _logger;
	private readonly SqlServerConnectionFactory _sqlServerConnectionFactory;
	private readonly PostgreSqlConnectionFactory _postgreSqlConnectionFactory;

	public TestConnectionTask(
		ILogger<TestConnectionTask> logger,
		SqlServerConnectionFactory sqlServerConnectionFactory,
		PostgreSqlConnectionFactory postgreSqlConnectionFactory
	)
	{
		_logger = logger;
		_sqlServerConnectionFactory = sqlServerConnectionFactory;
		_postgreSqlConnectionFactory = postgreSqlConnectionFactory;
	}

	public async Task Run()
	{
		await TestConnection(_sqlServerConnectionFactory);
		await TestConnection(_postgreSqlConnectionFactory);
	}

	private async Task TestConnection<T>(T connectionFactory) where T : IConnectionFactory
	{
		_logger.LogDebug("Trying to open connection with `{Type}`...", typeof(T).Name);
		using var connection = await connectionFactory.OpenAsync();
		connection.Close();
		_logger.LogInformation("Successful connection with `{Type}`", typeof(T).Name);
	}
}
