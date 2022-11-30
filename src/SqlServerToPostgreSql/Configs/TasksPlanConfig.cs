namespace SqlServerToPostgreSql.Configs;

public class TasksPlanConfig
{
	public bool CreateScheme { get; set; }

	public bool CreateTables { get; set; }

	public bool CreateTableConstraints { get; set; }

	public bool CreateTableIndexes { get; set; }

	public bool CopyRows { get; set; }

	public bool CreateRoles { get; set; }
}
