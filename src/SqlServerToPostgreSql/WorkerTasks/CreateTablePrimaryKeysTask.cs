using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Services;

namespace SqlServerToPostgreSql.WorkerTasks;

public class CreateTablePrimaryKeysTask : CreateTableConstraintsTask
{
	public CreateTablePrimaryKeysTask(ILogger<CreateTablePrimaryKeysTask> logger, WorkerTaskService service) : base("PRIMARY KEY", logger, service)
	{
	}
}
