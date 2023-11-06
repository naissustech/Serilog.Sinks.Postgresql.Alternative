// --------------------------------------------------------------------------------------------------------------------
// <copyright file="LogEventSerializedColumnWriter.cs" company="SeppPenner and the Serilog contributors">
// The project is licensed under the MIT license.
// </copyright>
// <summary>
//   This class is used to write the log event as JSON.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Serilog.Sinks.PostgreSQL.ColumnWriters;

/// <inheritdoc cref="ColumnWriterBase" />
/// <summary>
///     This class is used to write the log event as JSON.
/// </summary>
/// <seealso cref="ColumnWriterBase" />
public class LogEventSerializedColumnWriter : ColumnWriterBase
{
    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     Initializes a new instance of the <see cref="LogEventSerializedColumnWriter" /> class.
    /// </summary>
    public LogEventSerializedColumnWriter() : base(NpgsqlDbType.Jsonb, order: 0)
    {
    }

    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     Initializes a new instance of the <see cref="LogEventSerializedColumnWriter" /> class.
    /// </summary>
    /// <param name="dbType">The column type.</param>
    /// <param name="order">
    /// The order of the column writer if needed.
    /// Is used for sorting the columns as the writers are ordered alphabetically per default.
    /// </param>
    /// <seealso cref="ColumnWriterBase" />
    public LogEventSerializedColumnWriter(NpgsqlDbType dbType = NpgsqlDbType.Jsonb, int? order = null)
        : base(dbType, order: order)
    {
    }

    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     Gets the part of the log event to write to the column.
    /// </summary>
    /// <param name="logEvent">The log event.</param>
    /// <param name="formatProvider">The format provider.</param>
    /// <returns>
    ///     An object value.
    /// </returns>
    /// <seealso cref="ColumnWriterBase" />
    public override object GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        return LogEventToJson(logEvent, formatProvider);
    }

    /// <summary>
    ///     Converts the log event to json.
    /// </summary>
    /// <param name="logEvent">The log event.</param>
    /// <param name="formatProvider">The format provider.</param>
    /// <returns>The log event as json string.</returns>
    private static object LogEventToJson(LogEvent logEvent, IFormatProvider? formatProvider)
    {
        var jsonFormatter = new JsonFormatter(formatProvider: formatProvider);

        var sb = new StringBuilder();
        using var writer = new StringWriter(sb);
        jsonFormatter.Format(logEvent, writer);

        return sb.ToString();
    }
}
