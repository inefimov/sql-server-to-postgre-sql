namespace SqlServerToPostgreSql.Models;

public class SqlServerTableConstraint
{
	public string Name { get; set; } = "";

	public string Type { get; set; } = "";

	public string? UpdateRule { get; set; }

	public string? DeleteRule { get; set; }

	public string Columns { get; set; } = "";

	public string? RefTableName { get; set; }

	public string? RefColumns { get; set; }

	public string ToSql(string scheme, string tableName)
	{
		var schemeTable = $@"""{scheme}"".""{tableName}""";
		var name = $@"""{Name}""";
		var columns = string.Join(
			", ",
			Columns.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(x => $@"""{x.Trim()}""")
		);
		var reference = "";

		if (!string.IsNullOrEmpty(RefTableName))
		{
			var refSchemeTable = $@"""{scheme}"".""{RefTableName}""";
			var refColumns = string.Join(
				", ",
				RefColumns?.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(x => $@"""{x.Trim()}""") ?? Array.Empty<string>()
			);
			reference = $"references {refSchemeTable} ({refColumns})";
		}

		var rule = "";

		if (!string.IsNullOrEmpty(UpdateRule))
			rule += " on update " + UpdateRule;

		if (!string.IsNullOrEmpty(DeleteRule))
			rule += " on delete " + DeleteRule;

		return $@"
alter table {schemeTable} drop constraint if exists {name} cascade;
alter table {schemeTable} add constraint {name} {Type} ({columns}) {reference} {rule}
".Trim();
	}
}
