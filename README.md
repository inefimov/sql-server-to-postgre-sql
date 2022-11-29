# SqlServerToPostgreSql

Console application for migration from **SQL Server** to **PostgreSQL**.

## Instruction

1. Create an empty PostgreSQL database:
```sql
create database postgres_db;
```
2. Edit the settings in the `appsettings.json` file.
2. Launch the console application:
```shell
dotnet path/to/SqlServerToPostgreSql.dll
```

## Settings

Description of `appsettings.json`:

| Name                             | Description                                                                           |
|----------------------------------|---------------------------------------------------------------------------------------|
| ConnectionStrings.SqlServer      | The connection string to the SQL Server database.                                     |
| ConnectionStrings.PostgreSql     | The connection string to the PostgreSQL database.                                     |
|                                  |                                                                                       |
| SqlServer.SuperUser              | The name of the super database user.                                                  |
| SqlServer.Database               | The name of the database.                                                             |
| SqlServer.Scheme                 | The name of the database schema.                                                      |
| SqlServer.BatchSize              | The number of rows to be selected from the database table at a time.                  |
|                                  |                                                                                       |
| PostgreSql.SuperUser             | The name of the super database user.                                                  |
| PostgreSql.Database              | The name of the database.                                                             |
| PostgreSql.Scheme                | The name of the database schema.                                                      |
| PostgreSql.IncludeTables         | A list of table names to be copied.                                                   |
| PostgreSql.ExcludeTables         | A list of table names that should not be copied.                                      |
|                                  |                                                                                       |
| TasksPlan.CreateScheme           | Create a new database schema if necessary.                                            |
| TasksPlan.CreateTables           | Copy tables from SQL Server database to PostgreSQL database.                          |
| TasksPlan.CopyRows               | Copy all table rows from SQL Server to PostgreSQL.                                    |
| TasksPlan.CreateTableConstraints | Create all the necessary Primary, Foreign and Unique keys in the PostgreSQL database. |
| TasksPlan.CreateTableIndexes     | Create all the necessary indexes for the tables in the PostgreSQL database.           |
