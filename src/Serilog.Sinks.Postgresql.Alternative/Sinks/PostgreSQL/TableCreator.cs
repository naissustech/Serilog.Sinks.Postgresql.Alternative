// --------------------------------------------------------------------------------------------------------------------
// <copyright file="TableCreator.cs" company="SeppPenner and the Serilog contributors">
// The project is licensed under the MIT license.
// </copyright>
// <summary>
//   This class is used to create the tables.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Serilog.Sinks.PostgreSQL;

/// <summary>
///     This class is used to create the tables.
/// </summary>
public static class TableCreator
{
    /// <summary>
    ///     Creates the table.
    /// </summary>
    /// <param name="connection">The connection.</param>
    /// <param name="schemaName">The name of the schema.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="columnsInfo">The columns information.</param>
    public static async Task CreateTable(
        NpgsqlConnection connection,
        string schemaName,
        string tableName,
        IDictionary<string, ColumnWriterBase> columnsInfo)
    {
        using var command = connection.CreateCommand();
        command.CommandText = GetCreateTableQuery(schemaName, tableName, columnsInfo);
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    ///     Gets the create table query.
    /// </summary>
    /// <param name="schemaName">The name of the schema.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="columnsInfo">The columns information.</param>
    /// <returns>The create table query string.</returns>
    private static string GetCreateTableQuery(string schemaName, string tableName, IDictionary<string, ColumnWriterBase> columnsInfo)
    {
        schemaName = schemaName.Replace("\"", string.Empty);
        tableName = tableName.Replace("\"", string.Empty);

        var builder = new StringBuilder("CREATE TABLE IF NOT EXISTS ");

        if (!string.IsNullOrWhiteSpace(schemaName))
        {
            builder.Append('"');
            builder.Append(schemaName);
            builder.Append("\".");
        }

        builder.Append('"');
        builder.Append(tableName);
        builder.Append('"');
        builder.AppendLine(" (");

        builder.AppendLine(
            string.Join(",\n", columnsInfo.Select(r => $" \"{r.Key}\" {r.Value.GetSqlType()}")));

        builder.AppendLine(");");

        return builder.ToString();
    }
}
