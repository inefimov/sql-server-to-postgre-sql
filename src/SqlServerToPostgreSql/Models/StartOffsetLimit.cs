namespace SqlServerToPostgreSql.Models;

public class StartOffsetLimit
{
	public int Offset { get; set; }

	public int Limit { get; set; }

	public int NextPageIndex { get; set; }

	public static StartOffsetLimit From(int offset, int limit, int nextPageIndex)
		=> new()
		{
			Offset = offset,
			Limit = limit,
			NextPageIndex = nextPageIndex
		};
}