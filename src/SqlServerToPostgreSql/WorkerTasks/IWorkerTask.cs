namespace SqlServerToPostgreSql.WorkerTasks;

public interface IWorkerTask
{
	Task Run();
}
