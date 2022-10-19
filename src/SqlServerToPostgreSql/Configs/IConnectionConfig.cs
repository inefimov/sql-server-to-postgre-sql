namespace SqlServerToPostgreSql.Configs;

public interface IConnectionConfig
{
	string SuperUser { get; set; }

	string Database { get; set; }

	string Scheme { get; set; }
}
