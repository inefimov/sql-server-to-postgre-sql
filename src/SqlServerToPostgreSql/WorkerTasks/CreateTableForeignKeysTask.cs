using Microsoft.Extensions.Logging;
using SqlServerToPostgreSql.Services;

namespace SqlServerToPostgreSql.WorkerTasks;

public class CreateTableForeignKeysTask : CreateTableConstraintsTask
{
	public CreateTableForeignKeysTask(ILogger<CreateTableForeignKeysTask> logger, WorkerTaskService service) : base("FOREIGN KEY", logger, service)
	{
	}

	protected override string TableConstraintQuery => @"
select
	t.Name, t.Type,
	t.UpdateRule, t.DeleteRule, 
	right(t.Columns, len(t.Columns) - 1) as Columns,
	t.RefTableName,
	right(t.RefColumns, len(t.RefColumns) - 1) as RefColumns
from (
	select
		t.CONSTRAINT_NAME as 'Name',
		t.CONSTRAINT_TYPE as 'Type',
		iif(rc.UPDATE_RULE = 'NO ACTION', null, rc.UPDATE_RULE) as UpdateRule,
		iif(rc.DELETE_RULE = 'NO ACTION', null, rc.DELETE_RULE) as DeleteRule,
		(
			select
				',' + c.COLUMN_NAME
			from
				INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
			where
				c.CONSTRAINT_CATALOG = t.CONSTRAINT_CATALOG and c.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA and c.CONSTRAINT_NAME = t.CONSTRAINT_NAME and c.TABLE_NAME = t.TABLE_NAME
			order by
				c.ORDINAL_POSITION
			for xml path ('')
		) as Columns,
		(
			select top 1
				c.TABLE_NAME
			from
				INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
			where
				c.CONSTRAINT_CATALOG = rc.UNIQUE_CONSTRAINT_CATALOG and c.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA and c.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
		) as RefTableName,
		(
			select
				',' + c.COLUMN_NAME
			from
				INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
			where
				c.CONSTRAINT_CATALOG = rc.UNIQUE_CONSTRAINT_CATALOG and c.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA and c.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
			order by
				c.ORDINAL_POSITION
			for xml path ('')
		) as RefColumns
	from
		INFORMATION_SCHEMA.TABLE_CONSTRAINTS t,
		INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
	where
		t.CONSTRAINT_CATALOG = @tableCatalog
		and t.CONSTRAINT_SCHEMA = @tableSchema
		and t.CONSTRAINT_TYPE = @constraintType
		and t.TABLE_NAME = @tableName
		and rc.CONSTRAINT_CATALOG = t.CONSTRAINT_CATALOG
		and rc.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA
		and rc.CONSTRAINT_NAME = t.CONSTRAINT_NAME
) t
".Trim();
}
