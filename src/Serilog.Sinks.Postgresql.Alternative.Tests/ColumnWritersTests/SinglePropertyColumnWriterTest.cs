// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SinglePropertyColumnWriterTest.cs" company="SeppPenner and the Serilog contributors">
// The project is licensed under the MIT license.
// </copyright>
// <summary>
//   This class is used to test the <seealso cref="SinglePropertyColumnWriter" /> class.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Serilog.Sinks.Postgresql.Alternative.Tests.ColumnWritersTests;

/// <summary>
///     This class is used to test the <seealso cref="SinglePropertyColumnWriter" /> class.
/// </summary>
[TestClass]
public class SinglePropertyColumnWriterTest
{
    /// <summary>
    ///     This method is used to test the writer with not present properties.
    /// </summary>
    [TestMethod]
    public void PropertyIsNotPresentShouldReturnDbNullValue()
    {
        const string PropertyName = "TestProperty";

        var writer = new SinglePropertyColumnWriter(PropertyName, PropertyWriteMethod.ToString, format: "l");

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
    ///     This method is used to test the writer with the selected for scalar property.
    /// </summary>
    [TestMethod]
    public void RawSelectedForScalarPropertyShouldReturnPropertyValue()
    {
        const string PropertyName = "TestProperty";

        const int PropertyValue = 42;

        var property = new LogEventProperty(PropertyName, new ScalarValue(PropertyValue));

        var writer = new SinglePropertyColumnWriter(PropertyName, PropertyWriteMethod.Raw);

        var testEvent = new LogEvent(
            DateTime.Now,
            LogEventLevel.Debug,
            null,
            new MessageTemplate(Enumerable.Empty<MessageTemplateToken>()),
            new[] { property });

        var result = writer.GetValue(testEvent);

        Assert.AreEqual(PropertyValue, result);
    }

    /// <summary>
    ///     This method is used to test the writer with respected format.
    /// </summary>
    [TestMethod]
    public void WithToStringSelectedShouldRespectFormatPassed()
    {
        const string PropertyName = "TestProperty";

        const string PropertyValue = "TestValue";

        var property = new LogEventProperty(PropertyName, new ScalarValue(PropertyValue));

        var writer = new SinglePropertyColumnWriter(PropertyName, PropertyWriteMethod.ToString, format: "l");

        var testEvent = new LogEvent(
            DateTime.Now,
            LogEventLevel.Debug,
            null,
            new MessageTemplate(Enumerable.Empty<MessageTemplateToken>()),
            new[] { property });

        var result = writer.GetValue(testEvent);

        Assert.AreEqual(PropertyValue, result);
    }
}
