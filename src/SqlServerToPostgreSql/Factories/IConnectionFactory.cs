using System.Data;

namespace SqlServerToPostgreSql.Factories;

public interface IConnectionFactory
{
	Task<IDbConnection> OpenAsync();
}
