// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SinkHelper.cs" company="SeppPenner and the Serilog contributors">
// The project is licensed under the MIT license.
// </copyright>
// <summary>
//   The sink helper class to not duplicate the code in the audit sink.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Serilog.Sinks.PostgreSQL;

using Serilog.Parsing;
using System;
using System.Diagnostics;

/// <summary>
/// The sink helper class to not duplicate the code in the audit sink.
/// </summary>
public class SinkHelper
{
    /// <summary>
    ///     A boolean value indicating whether the table is created or not.
    /// </summary>
    private bool isTableCreated;

    /// <summary>
    ///     A boolean value indicating whether the schema is created or not.
    /// </summary>
    private bool isSchemaCreated;

    /// <summary>
    ///     Initializes a new instance of the <see cref="PostgreSqlSink" /> class.
    /// </summary>
    /// <param name="options">The sink options.</param>
    public SinkHelper(PostgreSqlOptions options)
    {
        this.SinkOptions = options;
        this.isSchemaCreated = !options.NeedAutoCreateSchema;
        this.isTableCreated = !options.NeedAutoCreateTable;
    }

    /// <summary>
    /// Gets or sets the PostgreSQL options.
    /// </summary>
    public PostgreSqlOptions SinkOptions { get; set; }

    /// <summary>
    /// Emits the events.
    /// </summary>
    /// <param name="events">The events.</param>
    public async Task Emit(IEnumerable<LogEvent> events)
    {
        var filteredEvents = this.FilterLogEvents(events);
        if (!filteredEvents.Any())
        {
            return;
        }

        filteredEvents = this.GenerateAndMergeManualLogs(filteredEvents);

        if (!filteredEvents.Any())
        {
            return;
        }

        await using var connection = new NpgsqlConnection(this.SinkOptions.ConnectionString);
        connection.Open();

        if (this.SinkOptions.NeedAutoCreateSchema && !this.isSchemaCreated && !string.IsNullOrWhiteSpace(this.SinkOptions.SchemaName))
        {
            await SchemaCreator.CreateSchema(connection, this.SinkOptions.SchemaName);
            this.isSchemaCreated = true;
        }

        if (this.SinkOptions.NeedAutoCreateTable && !this.isTableCreated && !string.IsNullOrWhiteSpace(this.SinkOptions.TableName))
        {
            if (this.SinkOptions.ColumnOptions.All(c => c.Value.Order != null))
            {
                var columnOptions = this.SinkOptions.ColumnOptions.OrderBy(c => c.Value.Order)
                    .ToDictionary(c => c.Key, x => x.Value);
                await TableCreator.CreateTable(connection, this.SinkOptions.SchemaName, this.SinkOptions.TableName, columnOptions);
            }
            else
            {
                await TableCreator.CreateTable(connection, this.SinkOptions.SchemaName, this.SinkOptions.TableName, this.SinkOptions.ColumnOptions);
            }

            this.isTableCreated = true;
        }

        if (this.SinkOptions.UseCopy)
        {
            await this.ProcessEventsByCopyCommand(filteredEvents, connection);
        }
        else
        {
            await this.ProcessEventsByInsertStatements(filteredEvents, connection);
        }
    }

    private List<LogEvent> FilterLogEvents(IEnumerable<LogEvent> events)
    {
        var filteredEvents = events
            .Where(e => (int)e.Level >= (int)LogEventLevel.Error ||
                        e.Properties.ContainsKey("SpanId") && (e.Properties.ContainsKey("ActionId") || e.Properties.ContainsKey("LoggedManually")))
            .ToList();

        return filteredEvents;
    }

    //private List<LogEvent> GenerateSpanIdsForErrorLogs(List<LogEvent> events)
    //{
    //    var differentRequestIds = events
    //        .Where(e => e.Level >= LogEventLevel.Error)
    //        .SelectMany(x => x.Properties.Where(p => p.Key == "RequestId").Select(p => p.Value.ToString()))
    //        .Distinct()
    //        .ToList();

    //    foreach (var requestId in differentRequestIds)
    //    {
    //        var eventsWithSameRequestId = events
    //            .Where(x => x.Properties.Any(p => p.Key == "RequestId" && p.Value.ToString() == requestId))
    //            .ToList();

    //        var foundSpanId = Activity.Current?.SpanId;
    //        if (foundSpanId != null)
    //        {
    //            _spanId = foundSpanId.Value;
    //            return;
    //        }

    //        _spanId = activitySpanId ?? new ActivitySpanId();

    //        foreach (var logEvent in eventsWithSameRequestId)
    //        {
                
    //        }
    //    }
    //}

    private List<LogEvent> GenerateAndMergeManualLogs(List<LogEvent> events)
    {
        var differentSpanIds = events
            .SelectMany(x => x.Properties.Where(p => p.Key == "SpanId").Select(p => p.Value.ToString()))
            .Distinct()
            .ToList();

        var newLogs = new List<LogEvent>();

        foreach (var spanId in differentSpanIds)
        {
            var automaticLogsError = false;

            var eventsWithSameSpanId = events
                .Where(x => x.Properties.Any(p => p.Key == "SpanId" && p.Value.ToString() == spanId))
                .ToList();

            if (!eventsWithSameSpanId.Any(x => x.Properties.ContainsKey("LoggedManually")))
            {
                var errorLogs = eventsWithSameSpanId.FindAll(x => x.Level >= LogEventLevel.Error);
                if (errorLogs.Any())
                {
                    automaticLogsError = true;
                }
                else
                {
                    continue;
                }

                //newLogs.AddRange(eventsWithSameSpanId.FindAll(x => x.Level >= LogEventLevel.Error));
                //continue;
            }

            var automaticLogs = eventsWithSameSpanId.Where(x => !x.Properties.ContainsKey("LoggedManually")).ToList();

            string? accountUid = null;
            string? clientUid = null;
            string? whitelabelUid = null;
            string? additionalMessage = null;
            DateTimeOffset timestamp = eventsWithSameSpanId.First().Timestamp;
            Exception? exception = null;
            LogEventPropertyValue? commandName;
            LogEventPropertyValue? userUid;
            LogEventPropertyValue? requestPath;
            LogEventLevel level = eventsWithSameSpanId.Max(x => x.Level);
            //LogEventPropertyValue request = null;

            var isError = level >= LogEventLevel.Error;

            userUid = eventsWithSameSpanId.Find(x => x.Properties.ContainsKey("UserId"))?.Properties.First(p => p.Key == "UserId").Value;

            requestPath = eventsWithSameSpanId.Find(x => x.Properties.ContainsKey("RequestPath"))?.Properties.First(p => p.Key == "RequestPath").Value;

            commandName = eventsWithSameSpanId.Find(x => x.Properties.ContainsKey("CommandName"))?.Properties.First(p => p.Key == "CommandName").Value;

            if (isError)
            {
                exception = eventsWithSameSpanId
                    .Where(x => x.Level >= LogEventLevel.Error)
                    .OrderByDescending(x => x.Level)
                    .ThenByDescending(x => x.Timestamp)
                    .First().Exception;
            }

            var requestProperties = new Dictionary<string, string>();

            this.FillProperties(automaticLogs, automaticLogsError, isError, ref requestProperties, ref accountUid, ref clientUid, ref whitelabelUid);

            var manualLogs = eventsWithSameSpanId.Where(x => x.Properties.ContainsKey("LoggedManually")).ToList();

            if (!requestProperties.Any())
            {
                this.FillProperties(manualLogs, automaticLogsError, isError, ref requestProperties, ref accountUid, ref clientUid, ref whitelabelUid);
            }

            var manualLogTokens = new List<MessageTemplateToken>();

            //var namedProperties = new Dictionary<string, string>();

            var tempLogs = manualLogs.Any() ? manualLogs : automaticLogs;
            foreach (var log in tempLogs)
            {
                var properties = log.Properties;
                foreach (var messageTemplateToken in log.MessageTemplate.Tokens)
                {
                    if (messageTemplateToken is PropertyToken propertyToken)
                    {
                        if (properties.TryGetValue(propertyToken.PropertyName, out var propertyValue))
                        {
                            //namedProperties[propertyToken.PropertyName] = propertyValue.ToString();
                            manualLogTokens.Add(new TextToken(propertyValue.ToString()));
                        }
                    }
                    else if (messageTemplateToken is TextToken)
                    {
                        manualLogTokens.Add(messageTemplateToken);
                    }
                }

                manualLogTokens.Add(new TextToken(Environment.NewLine));
            }

            if (automaticLogsError || isError)
            {
                if (requestProperties.Any())
                {
                    var errorTokens = automaticLogs.Where(x => x.Level >= LogEventLevel.Error).SelectMany(x => x.MessageTemplate.Tokens);

                    manualLogTokens.AddRange(errorTokens);
                    manualLogTokens.Add(new TextToken(Environment.NewLine));

                    manualLogTokens.Add(new TextToken("Request:"));
                    manualLogTokens.Add(new TextToken(Environment.NewLine));

                    foreach (var requestProperty in requestProperties)
                    {
                        manualLogTokens.Add(new TextToken($"\t{requestProperty.Key}: "));
                        manualLogTokens.Add(new PropertyToken(requestProperty.Key, requestProperty.Value));
                        manualLogTokens.Add(new TextToken(Environment.NewLine));
                    }
                }
            }

            var newMessageTemplate = new MessageTemplate(manualLogTokens);

            var actionName =
                manualLogs.Find(x => x.Properties.ContainsKey("ActionName"))?.Properties.First(p => p.Key == "ActionName").Value;
            //(x => x.Properties.FirstOrDefault(p => p.Key == "ActionName"))?.FirstOrDefault()?.Value

            var actionId =
                manualLogs.Find(x => x.Properties.ContainsKey("ActionId"))?.Properties.First(p => p.Key == "ActionId").Value;

            var spanIdProp =
                eventsWithSameSpanId.Find(x => x.Properties.ContainsKey("SpanId")).Properties.First(p => p.Key == "SpanId").Value;

            if (level == LogEventLevel.Warning)
            {
                var warnings = eventsWithSameSpanId.Where(x => x.Level == LogEventLevel.Warning).ToList();
                var additionalMessageBuilder = new StringBuilder();

                foreach (var log in warnings)
                {
                    var properties = log.Properties;
                    foreach (var messageTemplateToken in log.MessageTemplate.Tokens)
                    {
                        if (messageTemplateToken is PropertyToken propertyToken)
                        {
                            if (properties.TryGetValue(propertyToken.PropertyName, out var propertyValue))
                            {
                                additionalMessageBuilder.Append(propertyValue);
                            }
                        }
                        else if (messageTemplateToken is TextToken)
                        {
                            additionalMessageBuilder.Append(messageTemplateToken);
                        }
                    }

                    additionalMessageBuilder.Append(Environment.NewLine);
                }

                additionalMessage = additionalMessageBuilder.ToString();
            }

            var newProperties = new List<LogEventProperty>
                {
                    new("SpanId", spanIdProp),
                    new("LoggedManually", new ScalarValue(true)),
                };

            if (actionName != null)
            {
                newProperties.Add(new LogEventProperty("ActionName", actionName));
            }

            if (actionId != null)
            {
                newProperties.Add(new LogEventProperty("ActionId", actionId));
            }

            if (!string.IsNullOrEmpty(whitelabelUid))
            {
                newProperties.Add(new LogEventProperty("WhitelabelUid", new ScalarValue(whitelabelUid)));
            }
            else
            {
                var whitelabelUidProp =
                    manualLogs.Find(x => x.Properties.ContainsKey("WhitelabelCompanyUid"))?.Properties.First(p => p.Key == "WhitelabelCompanyUid").Value;

                whitelabelUidProp ??= manualLogs.Find(x => x.Properties.ContainsKey("WhitelabelUid"))?.Properties.First(p => p.Key == "WhitelabelUid").Value;

                whitelabelUid = whitelabelUidProp?.ToString();

                if (string.IsNullOrEmpty(whitelabelUid))
                {
                    requestProperties.TryGetValue("TennantUid", out whitelabelUid);
                }

                if (!string.IsNullOrEmpty(whitelabelUid))
                {
                    newProperties.Add(new LogEventProperty("WhitelabelUid", new ScalarValue(whitelabelUid)));
                }
            }

            if (!string.IsNullOrEmpty(clientUid))
            {
                newProperties.Add(new LogEventProperty("ClientUid", new ScalarValue(clientUid)));
            }
            else
            {
                var clientUidProp =
                    manualLogs.Find(x => x.Properties.ContainsKey("ClientUid"))?.Properties.First(p => p.Key == "ClientUid").Value;

                clientUidProp ??= eventsWithSameSpanId.Find(x => x.Properties.ContainsKey("ClientUid"))?.Properties.First(p => p.Key == "ClientUid").Value;

                if (clientUidProp != null)
                {
                    newProperties.Add(new LogEventProperty("ClientUid", clientUidProp));
                }
                else
                {
                    clientUid = clientUidProp?.ToString();

                    if (string.IsNullOrEmpty(clientUid))
                    {
                        requestProperties.TryGetValue("ClientUid", out clientUid);
                    }

                    if (!string.IsNullOrEmpty(clientUid))
                    {
                        newProperties.Add(new LogEventProperty("ClientUid", new ScalarValue(clientUid)));
                    }

                    if (requestProperties.TryGetValue("ClientUid", out clientUid))
                    {
                        newProperties.Add(new LogEventProperty("ClientUid", new ScalarValue(clientUid)));
                    }
                }
            }

            if (!string.IsNullOrEmpty(accountUid))
            {
                newProperties.Add(new LogEventProperty("AccountUid", new ScalarValue(accountUid)));
            }
            else
            {
                var accountUidProp =
                    manualLogs.Find(x => x.Properties.ContainsKey("AccountUid"))?.Properties.First(p => p.Key == "AccountUid").Value;

                accountUidProp ??= eventsWithSameSpanId.Find(x => x.Properties.ContainsKey("AccountUid"))?.Properties
                    .First(p => p.Key == "AccountUid").Value;

                if (accountUidProp != null)
                {
                    newProperties.Add(new LogEventProperty("AccountUid", accountUidProp));
                }
                else
                {
                    accountUid = accountUidProp?.ToString();

                    if (string.IsNullOrEmpty(accountUid))
                    {
                        requestProperties.TryGetValue("AccountUid", out accountUid);
                    }

                    if (!string.IsNullOrEmpty(accountUid))
                    {
                        newProperties.Add(new LogEventProperty("AccountUid", new ScalarValue(accountUid)));
                    }

                    if (requestProperties.TryGetValue("AccountUid", out accountUid))
                    {
                        newProperties.Add(new LogEventProperty("AccountUid", new ScalarValue(accountUid)));
                    }
                }
            }

            if (userUid != null)
            {
                newProperties.Add(new LogEventProperty("UserId", userUid));
            }
            else
            {
                if (requestProperties.TryGetValue("UserId", out var userUidString))
                {
                    newProperties.Add(new LogEventProperty("UserId", new ScalarValue(userUidString)));
                }
            }

            if (commandName != null)
            {
                newProperties.Add(new LogEventProperty("Name", commandName));
                newProperties.Add(new LogEventProperty("CommandName", commandName));
            }
            else
            {
                if (requestProperties.TryGetValue("CommandName", out var commandNameString))
                {
                    newProperties.Add(new LogEventProperty("Name", new ScalarValue(commandNameString)));
                    newProperties.Add(new LogEventProperty("CommandName", new ScalarValue(commandNameString)));
                }
            }

            //if (request != null)
            //{
            //    newProperties.Add(new LogEventProperty("Request", request));
            //}

            if (requestPath != null)
            {
                newProperties.Add(new LogEventProperty("RequestPath", requestPath));
            }

            if (!string.IsNullOrEmpty(additionalMessage))
            {
                newProperties.Add(new LogEventProperty("AdditionalMessage", new ScalarValue(additionalMessage)));
            }

            var newLog = new LogEvent(timestamp, level, exception, newMessageTemplate, newProperties);

            newLogs.Add(newLog);
        }

        return newLogs;
    }

    private void FillProperties(List<LogEvent> logs,
        bool automaticLogsError,
        bool isError,
        ref Dictionary<string, string> requestProperties,
        ref string? accountUid,
        ref string? clientUid,
        ref string? whitelabelUid)
    {
        foreach (var automaticLog in logs)
        {
            if (!automaticLog.Properties.ContainsKey("Request"))
            {
                continue;
            }

            var logEventPropertyValue = automaticLog.Properties["Request"];
            if (logEventPropertyValue == null)
            {
                continue;
            }

            //request ??= logEventPropertyValue;

            switch (logEventPropertyValue)
            {
                case ScalarValue:
                    break;
                case SequenceValue:
                    break;
                case StructureValue structure:
                    var structureProperties = structure.Properties;

                    var accountIdentifierProperties =
                        ((StructureValue?)structureProperties.FirstOrDefault(x => x.Name == "AccountIdentifier")?.Value)?.Properties;

                    if (automaticLogsError || isError)
                    {
                        foreach (var structureProperty in structureProperties)
                        {
                            requestProperties[structureProperty.Name] = structureProperty.PropertyToString();
                        }
                    }

                    if (accountIdentifierProperties == null || automaticLog.Properties.ContainsKey("LoggedManually"))
                    {
                        continue;
                    }

                    accountUid = accountIdentifierProperties.FirstOrDefault(x => x.Name == "AccountUid")?.Value.ToString();

                    clientUid = accountIdentifierProperties.FirstOrDefault(x => x.Name == "ClientUid")?.Value.ToString();

                    whitelabelUid = accountIdentifierProperties.FirstOrDefault(x => x.Name == "WhitelabelCompanyUid")?.Value.ToString();
                    break;
                case DictionaryValue:
                    break;
            }
        }
    }

    /// <summary>
    ///     Processes the events by the copy command.
    /// </summary>
    /// <param name="events">The events.</param>
    /// <param name="connection">The connection.</param>
    public async Task ProcessEventsByCopyCommand(IEnumerable<LogEvent> events, NpgsqlConnection connection)
    {
        using var binaryCopyWriter = connection.BeginBinaryImport(this.GetCopyCommand());
        this.WriteToStream(binaryCopyWriter, events);
        await binaryCopyWriter.CompleteAsync();
    }

    /// <summary>
    ///     Processes the events by insert statements.
    /// </summary>
    /// <param name="events">The events.</param>
    /// <param name="connection">The connection.</param>
    public async Task ProcessEventsByInsertStatements(IEnumerable<LogEvent> events, NpgsqlConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = this.GetInsertQuery();

        foreach (var logEvent in events)
        {
            command.Parameters.Clear();
            foreach (var columnKey in this.ColumnNamesWithoutSkipped())
            {
                command.Parameters.AddWithValue(
                    ClearColumnNameForParameterName(columnKey),
                    this.SinkOptions.ColumnOptions[columnKey].DbType,
                    this.SinkOptions.ColumnOptions[columnKey].GetValue(logEvent, this.SinkOptions.FormatProvider) ?? DBNull.Value);
            }

            await command.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    ///     Clears the name of the column name for parameter.
    /// </summary>
    /// <param name="columnName">Name of the column.</param>
    /// <returns>The cleared column name.</returns>
    private static string ClearColumnNameForParameterName(string columnName)
    {
        return columnName?.Replace("\"", string.Empty) ?? string.Empty;
    }

    /// <summary>
    ///     Gets the copy command.
    /// </summary>
    /// <returns>A SQL string with the copy command.</returns>
    private string GetCopyCommand()
    {
        var columns = "\"" + string.Join("\", \"", this.ColumnNamesWithoutSkipped()) + "\"";
        var builder = new StringBuilder();
        builder.Append("COPY ");

        if (!string.IsNullOrWhiteSpace(this.SinkOptions.SchemaName))
        {
            builder.Append('"');
            builder.Append(this.SinkOptions.SchemaName);
            builder.Append("\".");
        }

        builder.Append('"');
        builder.Append(this.SinkOptions.TableName);
        builder.Append('"');

        builder.Append(" (");
        builder.Append(columns);
        builder.Append(") FROM STDIN BINARY;");
        return builder.ToString();
    }

    /// <summary>
    ///     Gets the insert query.
    /// </summary>
    /// <returns>A SQL string with the insert query.</returns>
    private string GetInsertQuery()
    {
        var columns = "\"" + string.Join("\", \"", this.ColumnNamesWithoutSkipped()) + "\"";

        var parameters = string.Join(
            ", ",
            this.ColumnNamesWithoutSkipped().Select(cn => "@" + ClearColumnNameForParameterName(cn)));

        var builder = new StringBuilder();
        builder.Append("INSERT INTO ");

        if (!string.IsNullOrWhiteSpace(this.SinkOptions.SchemaName))
        {
            builder.Append('"');
            builder.Append(this.SinkOptions.SchemaName);
            builder.Append("\".");
        }

        builder.Append('"');
        builder.Append(this.SinkOptions.TableName);
        builder.Append('"');

        builder.Append(" (");
        builder.Append(columns);
        builder.Append(") VALUES (");
        builder.Append(parameters);
        builder.Append(");");
        return builder.ToString();
    }

    /// <summary>
    ///     Writes to the stream.
    /// </summary>
    /// <param name="writer">The writer.</param>
    /// <param name="entities">The entities.</param>
    private void WriteToStream(NpgsqlBinaryImporter writer, IEnumerable<LogEvent> entities)
    {
        foreach (var entity in entities)
        {
            writer.StartRow();

            foreach (var columnKey in this.ColumnNamesWithoutSkipped())
            {
                var value = this.SinkOptions.ColumnOptions[columnKey].GetValue(entity, this.SinkOptions.FormatProvider);
                var dbType = this.SinkOptions.ColumnOptions[columnKey].DbType;

                if (dbType == NpgsqlDbType.Text && value is string stringValue)
                {
                    if (stringValue.StartsWith("\"") && stringValue.EndsWith("\"") && stringValue.Length > 1)
                    {
                        value = stringValue.Substring(1, stringValue.Length - 2);
                    }
                }

                writer.Write(value, dbType);
            }
        }
    }

    /// <summary>
    /// The columns names without skipped columns.
    /// </summary>
    /// <returns>The list of column names for the INSERT query.</returns>
    private IEnumerable<string> ColumnNamesWithoutSkipped() =>
        this.SinkOptions.ColumnOptions
            .Where(c => !c.Value.SkipOnInsert)
            .Select(c => c.Key);
}
