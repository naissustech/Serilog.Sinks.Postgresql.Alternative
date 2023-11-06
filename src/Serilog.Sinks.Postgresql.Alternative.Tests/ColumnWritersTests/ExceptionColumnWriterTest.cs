// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ExceptionColumnWriterTest.cs" company="SeppPenner and the Serilog contributors">
// The project is licensed under the MIT license.
// </copyright>
// <summary>
//   This class is used to test the <seealso cref="ExceptionColumnWriter" /> class.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Serilog.Sinks.Postgresql.Alternative.Tests.ColumnWritersTests;

/// <summary>
///     This class is used to test the <seealso cref="ExceptionColumnWriter" /> class.
/// </summary>
[TestClass]
public class ExceptionColumnWriterTest
{
    /// <summary>
    ///     This method is used to test the writer with empty exceptions.
    /// </summary>
    [TestMethod]
    public void ExceptionIsNullShouldReturnDbNullValue()
    {
        var writer = new ExceptionColumnWriter();

        var testEvent = new LogEvent(
            DateTime.Now,
            LogEventLevel.Debug,
            null,
            new MessageTemplate(Enumerable.Empty<MessageTemplateToken>()),
            Enumerable.Empty<LogEventProperty>());

        var result = writer.GetValue(testEvent);

        Assert.AreEqual(DBNull.Value, result);
    }

    /// <summary>
    ///     This method is used to test the writer with valid exceptions.
    /// </summary>
    [TestMethod]
    public void ExceptionIsPresentShouldReturnStringRepresentation()
    {
        var writer = new ExceptionColumnWriter();

        var exception = new Exception("Test exception");

        var testEvent = new LogEvent(
            DateTime.Now,
            LogEventLevel.Debug,
            exception,
            new MessageTemplate(Enumerable.Empty<MessageTemplateToken>()),
            Enumerable.Empty<LogEventProperty>());

        var result = writer.GetValue(testEvent);

        Assert.AreEqual(exception.ToString(), result);
    }
}
