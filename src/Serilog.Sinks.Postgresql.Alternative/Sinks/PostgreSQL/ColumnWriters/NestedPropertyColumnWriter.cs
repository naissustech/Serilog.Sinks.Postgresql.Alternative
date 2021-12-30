using NpgsqlTypes;
using Serilog.Events;
using Serilog.Formatting.Json;
using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;

namespace Serilog.Sinks.PostgreSQL.ColumnWriters
{
    /// <inheritdoc cref="ColumnWriterBase" />
    /// <summary>
    ///     This class is used to write a nested event property.
    /// </summary>
    /// <seealso cref="ColumnWriterBase" />
    public class NestedPropertyColumnWriter : ColumnWriterBase
    {
        /// <inheritdoc cref="ColumnWriterBase" />
        /// <summary>
        ///     Initializes a new instance of the <see cref="NestedPropertyColumnWriter" /> class.
        /// </summary>
        public NestedPropertyColumnWriter() : base(NpgsqlDbType.Text, order: 0)
        {
        }

        /// <inheritdoc cref="ColumnWriterBase" />
        /// <summary>
        ///     Initializes a new instance of the <see cref="NestedPropertyColumnWriter" /> class.
        /// </summary>
        /// <param name="order">
        /// The order of the column writer if needed.
        /// Is used for sorting the columns as the writers are ordered alphabetically per default.
        /// </param>
        /// <seealso cref="ColumnWriterBase" />
        // ReSharper disable once UnusedMember.Global
        public NestedPropertyColumnWriter(int? order = null) : base(NpgsqlDbType.Text, order: order)
        {
        }

        /// <inheritdoc cref="ColumnWriterBase" />
        /// <summary>
        ///     Initializes a new instance of the <see cref="NestedPropertyColumnWriter" /> class.
        /// </summary>
        /// <param name="parentPropertyName">Name of the parent property.</param>
        /// <param name="nestedPropertyName">Name of the nested property.</param>
        /// <param name="useExactNestedPropertyName">Should use exact nested property name</param>
        /// <param name="writeMethod">The write method.</param>
        /// <param name="dbType">Type of the database.</param>
        /// <param name="format">The format.</param>
        /// <param name="order">
        /// The order of the column writer if needed.
        /// Is used for sorting the columns as the writers are ordered alphabetically per default.
        /// </param>
        /// <seealso cref="ColumnWriterBase" />
        [SuppressMessage(
            "StyleCop.CSharp.NamingRules",
            "SA1305:FieldNamesMustNotUseHungarianNotation",
            Justification = "Reviewed. Suppression is OK here.")]
        // ReSharper disable once UnusedMember.Global
        public NestedPropertyColumnWriter(
            string parentPropertyName,
            string nestedPropertyName,
            bool useExactNestedPropertyName,
            PropertyWriteMethod writeMethod = PropertyWriteMethod.ToString,
            NpgsqlDbType dbType = NpgsqlDbType.Text,
            string format = null,
            int? order = null)
            : base(dbType, order: order)
        {
            ParentName = parentPropertyName;
            NestedName = nestedPropertyName;
            WriteMethod = writeMethod;
            Format = format;
            UseExactNestedPropertyName = useExactNestedPropertyName;
        }

        /// <summary>
        ///     Gets or sets the format.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public string Format { get; set; }

        /// <summary>
        ///     Gets or sets the parent name.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public string ParentName { get; set; }

        /// <summary>
        ///     Gets or sets the nested name.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public string NestedName { get; set; }

        /// <summary>
        ///     Gets or sets the use exact nested property name
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public bool UseExactNestedPropertyName { get; set; }

        /// <summary>
        ///     Gets or sets the write method.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public PropertyWriteMethod WriteMethod { get; set; }

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
        public override object GetValue(LogEvent logEvent, IFormatProvider formatProvider = null)
        {
            if (!logEvent.Properties.ContainsKey(ParentName))
            {
                return DBNull.Value;
            }

            var parentProperty = logEvent.Properties[ParentName] as StructureValue;
            if (parentProperty == null)
            {
                return DBNull.Value;
            }

            var nestedProperties = parentProperty.Properties as LogEventProperty[];
            if (nestedProperties == null)
            {
                return DBNull.Value;
            }

            LogEventProperty nestedPropery = null;
            if (UseExactNestedPropertyName)
            {
                nestedPropery = nestedProperties.FirstOrDefault(x => x.Name == NestedName);
            }
            else
            {
                nestedPropery = nestedProperties.FirstOrDefault(x => x.Name.IndexOf(NestedName, StringComparison.OrdinalIgnoreCase) >= 0);
            }

            if (nestedPropery == null)
            {
                return DBNull.Value;
            }

            var nestedValue = nestedPropery.Value;

            // ReSharper disable once SwitchStatementMissingSomeCases
            switch (WriteMethod)
            {
                case PropertyWriteMethod.Raw:
                    var value = GetPropertyValue(nestedValue);
                    if (value == null)
                    {
                        return DBNull.Value;
                    }
                    return value;
                case PropertyWriteMethod.ToString:
                    value = GetPropertyValue(nestedValue);
                    if (value == null)
                    {
                        return DBNull.Value;
                    }
                    return value.ToString();
                case PropertyWriteMethod.Json:
                    var valuesFormatter = new JsonValueFormatter();
                    var sb = new StringBuilder();
                    using (var writer = new StringWriter(sb))
                    {
                        valuesFormatter.Format(nestedValue, writer);
                    }
                    return sb.ToString();
                default:
                    return nestedValue.ToString(Format, formatProvider);
            }
        }

        /// <summary>
        ///     Gets the property value.
        /// </summary>
        /// <param name="logEventProperty">The log event property.</param>
        /// <returns>The property value.</returns>
        private static object GetPropertyValue(LogEventPropertyValue logEventProperty)
        {
            // TODO: Add support for arrays
            if (logEventProperty is ScalarValue scalarValue)
            {
                return scalarValue.Value;
            }

            return logEventProperty;
        }
    }
}
