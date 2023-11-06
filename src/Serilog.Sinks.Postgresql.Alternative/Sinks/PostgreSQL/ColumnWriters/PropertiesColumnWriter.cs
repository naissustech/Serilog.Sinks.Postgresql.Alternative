// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PropertiesColumnWriter.cs" company="SeppPenner and the Serilog contributors">
// The project is licensed under the MIT license.
// </copyright>
// <summary>
//   This class is used to write all event properties.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Serilog.Sinks.PostgreSQL.ColumnWriters;

/// <inheritdoc cref="ColumnWriterBase" />
/// <summary>
///     This class is used to write all event properties.
/// </summary>
/// <seealso cref="ColumnWriterBase" />
public class PropertiesColumnWriter : ColumnWriterBase
{
    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     Initializes a new instance of the <see cref="PropertiesColumnWriter" /> class.
    /// </summary>
    public PropertiesColumnWriter() : base(NpgsqlDbType.Jsonb, order: 0)
    {
    }

    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     Initializes a new instance of the <see cref="PropertiesColumnWriter" /> class.
    /// </summary>
    /// <param name="dbType">The column type.</param>
    /// <param name="order">
    /// The order of the column writer if needed.
    /// Is used for sorting the columns as the writers are ordered alphabetically per default.
    /// </param>
    /// <seealso cref="ColumnWriterBase" />
    public PropertiesColumnWriter(NpgsqlDbType dbType = NpgsqlDbType.Jsonb, int? order = null)
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
        return PropertiesToJson(logEvent);
    }

    /// <summary>
    ///     Converts the properties to json.
    /// </summary>
    /// <param name="logEvent">The log event.</param>
    /// <returns>The properties as json object.</returns>
    private static object PropertiesToJson(LogEvent logEvent)
    {
        if (logEvent.Properties.Count == 0)
        {
            return "{}";
        }

        var valuesFormatter = new JsonValueFormatter();

        var sb = new StringBuilder();

        sb.Append('{');

        using (var writer = new StringWriter(sb))
        {
            foreach (var keyValuePair in logEvent.Properties)
            {
                sb.Append($"\"{keyValuePair.Key}\":");
                valuesFormatter.Format(keyValuePair.Value, writer);
                sb.Append(", ");
            }
        }

        sb.Remove(sb.Length - 2, 2);
        sb.Append('}');

        return sb.ToString();
    }
}
