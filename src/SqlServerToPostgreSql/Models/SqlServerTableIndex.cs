namespace SqlServerToPostgreSql.Models;

public class SqlServerTableIndex
{
	public string Name { get; set; } = "";

	public bool IsUnique { get; set; }

	public string Columns { get; set; } = "";

	public string? IncludedColumns { get; set; }

	public string ToSql(string scheme, string tableName)
	{
		var schemeTable = $@"""{scheme}"".""{tableName}""";
		var unique = IsUnique ? "unique" : "";
		var columns = string.Join(", ", Columns.Split(',', StringSplitOptions.RemoveEmptyEntries));
		var includedColumns = string.Join(", ", IncludedColumns?.Split(',', StringSplitOptions.RemoveEmptyEntries) ?? Array.Empty<string>());

		if (!string.IsNullOrEmpty(includedColumns))
			includedColumns = $"include ({includedColumns})";

		var name = string.Join("_", new[]
		{
			"IX",
			tableName.ToLower(),
			string.Join("_", Columns.Split(',', StringSplitOptions.RemoveEmptyEntries)).Replace(" desc", "").Replace("\"", "").ToLower(),
			string.Join("_", IncludedColumns?.Split(',', StringSplitOptions.RemoveEmptyEntries) ?? Array.Empty<string>()).Replace(" desc", "").Replace("\"", "").ToLower()
		}.Where(x => !string.IsNullOrEmpty(x)));

		return $@"
drop index if exists {scheme}.{name} cascade;
create {unique} index {name} on {schemeTable} ({columns}) {includedColumns}
".Trim();
	}
}
