using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.WorkerTasks;
using Tenogy.App;

namespace SqlServerToPostgreSql;

public static class Props
{
	public const int CommandTimeout = 18000;

	public const int BatchSize = 10000;

	public const int InsertPageSize = 1000;

	public const string SpaceMs = "    MS:";

	public const string SpacePg = "    PG:";
}

public class Worker : IWorker
{
	private readonly ILogger<Worker> _logger;
	private readonly IEnumerable<IWorkerTask> _workerTasks;

	public Worker(
		ILogger<Worker> logger,
		IEnumerable<IWorkerTask> workerTasks
	)
	{
		_logger = logger;
		_workerTasks = workerTasks;
	}

	public async Task DoWork(CancellationToken cancellationToken)
	{
		_logger.LogInformation("Start worker...");

		foreach (var workerTask in _workerTasks)
		{
			if (cancellationToken.IsCancellationRequested)
			{
				_logger.LogInformation("Aborted worker `{TaskName}`", workerTask.GetType().Name);
				return;
			}

			await workerTask.Run();
		}

		_logger.LogInformation("Stop worker");
	}
}
