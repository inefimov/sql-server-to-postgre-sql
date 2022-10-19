using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Services;

namespace SqlServerToPostgreSql.WorkerTasks;

public class CreateTableUniqueKeysTask : CreateTableConstraintsTask
{
	public CreateTableUniqueKeysTask(ILogger<CreateTableUniqueKeysTask> logger, WorkerTaskService service) : base("UNIQUE", logger, service)
	{
	}
}
