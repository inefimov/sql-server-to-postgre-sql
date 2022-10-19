namespace SqlServerToPostgreSql.Configs;

public class PostgreSqlConfig : IConnectionConfig
{
	public string SuperUser { get; set; } = "";

	public string Database { get; set; } = "";

	public string Scheme { get; set; } = "";

	public IEnumerable<string>? IncludeTables { get; set; }

	public IEnumerable<string>? ExcludeTables { get; set; }
}
