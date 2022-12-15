using Serilog.Events;

namespace Serilog.Sinks.PostgreSQL;

/// <summary>
/// Extension methods for Log Properties
/// </summary>
public static class LogPropertyExtension
{
    /// <summary>
    /// Extract string value of LogEventProperty
    /// </summary>
    /// <param name="logEventProperty">LogEventProperty to extract string value</param>
    /// <returns>String vale of property</returns>
    public static string PropertyToString(this LogEventProperty logEventProperty)
    {
        var stringValue = logEventProperty.Value?.ToString();
        if (string.IsNullOrEmpty(stringValue))
        {
            return string.Empty;
        }

        if (stringValue.StartsWith("\"") && stringValue.EndsWith("\"") && stringValue.Length > 1)
        {
            return stringValue.Substring(1, stringValue.Length - 2);
        }

        return stringValue;
    }
}