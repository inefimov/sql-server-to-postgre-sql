using SqlServerToPostgreSql;
using SqlServerToPostgreSql.Configs;
using SqlServerToPostgreSql.Factories;
using SqlServerToPostgreSql.Services;
using SqlServerToPostgreSql.WorkerTasks;
using Tenogy.App;
using Tenogy.SimpleInjector;

await App.New(args).RunOneTimeWorker<Worker>(container =>
{
	container.RegisterConfig<SqlServerConfig>("SqlServer");
	container.RegisterConfig<PostgreSqlConfig>("PostgreSql");
	container.RegisterConfig<TasksPlanConfig>("TasksPlan");

	container.RegisterSingleton<SqlServerConnectionFactory>();
	container.RegisterSingleton<PostgreSqlConnectionFactory>();

	container.RegisterSingleton<WorkerTaskService>();

	container.Collection.Register(typeof(IWorkerTask), new[]
	{
		typeof(TestConnectionTask),
		typeof(CreateSchemeTask),
		typeof(CreateTablesTask),
		typeof(CopyRowsTask),
		typeof(CreateTablePrimaryKeysTask),
		typeof(CreateTableUniqueKeysTask),
		typeof(CreateTableForeignKeysTask),
		typeof(CreateTableIndexesTask),
		typeof(CreateRolesTask),
	});
});
