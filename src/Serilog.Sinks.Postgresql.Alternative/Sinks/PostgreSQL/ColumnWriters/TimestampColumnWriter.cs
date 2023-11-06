// --------------------------------------------------------------------------------------------------------------------
// <copyright file="TimestampColumnWriter.cs" company="SeppPenner and the Serilog contributors">
// The project is licensed under the MIT license.
// </copyright>
// <summary>
//   This class is used to write the timestamp.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Serilog.Sinks.PostgreSQL.ColumnWriters;

/// <inheritdoc cref="ColumnWriterBase" />
/// <summary>
///     This class is used to write the timestamp.
/// </summary>
/// <seealso cref="ColumnWriterBase" />
public class TimestampColumnWriter : ColumnWriterBase
{
    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     Initializes a new instance of the <see cref="TimestampColumnWriter" /> class.
    /// </summary>
    public TimestampColumnWriter() : base(NpgsqlDbType.TimestampTz, order: 0)
    {
    }

    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     Initializes a new instance of the <see cref="TimestampColumnWriter" /> class.
    /// </summary>
    /// <param name="dbType">The column type.</param>
    /// <param name="order">
    /// The order of the column writer if needed.
    /// Is used for sorting the columns as the writers are ordered alphabetically per default.
    /// </param>
    /// <seealso cref="ColumnWriterBase" />
    public TimestampColumnWriter(NpgsqlDbType dbType = NpgsqlDbType.TimestampTz, int? order = null)
        : base(dbType, order: order)
    {
        // Set the DbType to NpgsqlDbType.TimestampTz in any case: Check https://github.com/npgsql/npgsql/issues/2470 for more details.
        this.DbType = NpgsqlDbType.TimestampTz;
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
        return logEvent.Timestamp.ToUniversalTime();
    }
}
