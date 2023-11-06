// --------------------------------------------------------------------------------------------------------------------
// <copyright file="IdAutoIncrementColumnWriter.cs" company="SeppPenner and the Serilog contributors">
// The project is licensed under the MIT license.
// </copyright>
// <summary>
//   This class is used to write columns with auto increment and primary key.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Serilog.Sinks.PostgreSQL.ColumnWriters;

/// <inheritdoc cref="ColumnWriterBase" />
/// <summary>
///     This class is used to write columns with auto increment and primary key.
/// </summary>
/// <seealso cref="ColumnWriterBase" />
public class IdAutoIncrementColumnWriter : ColumnWriterBase
{
    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     Initializes a new instance of the <see cref="IdAutoIncrementColumnWriter" /> class.
    /// </summary>
    public IdAutoIncrementColumnWriter() : base(NpgsqlDbType.Bigint, true, 0)
    {
    }

    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     Initializes a new instance of the <see cref="ColumnWriterBase" /> class.
    /// </summary>
    /// <param name="order">
    /// The order of the column writer if needed.
    /// Is used for sorting the columns as the writers are ordered alphabetically per default.
    /// </param>
    /// <seealso cref="ColumnWriterBase"/>
    public IdAutoIncrementColumnWriter(int? order = null) : base(NpgsqlDbType.Bigint, true, order)
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
    /// <seealso cref="ColumnWriterBase"/>
    public override object GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        // this method should not be called, because of the autoincrement
        throw new Exception("Auto-increment column should not have a value to be written!");
    }

    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    /// Gets the type of the SQL query.
    /// </summary>
    /// <returns>The PostgreSql type for inserting it into the CREATE TABLE query.</returns>
    /// <seealso cref="ColumnWriterBase"/>
    public override string GetSqlType()
    {
        return "SERIAL PRIMARY KEY";
    }
}
