// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ODataResultTableV2
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class ODataResultTableV2 : ODataResultTable
{
  private static DateTime InitDateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
  private static long MinMilliseconds = checked ((long) (unchecked (DateTime.MinValue - ODataResultTableV2.InitDateTime)).TotalMilliseconds);
  private static long MaxMilliseconds = checked ((long) (unchecked (DateTime.MaxValue - ODataResultTableV2.InitDateTime)).TotalMilliseconds);

  internal ODataResultTableV2(ODataExtractionDefinition definition, IList<object?[]> results)
    : base(definition, results)
  {
  }

  private static DateTime ParseAndReturnDateTime(string value)
  {
    int num1 = value.IndexOf("(", StringComparison.Ordinal);
    int num2 = value.IndexOf(")", StringComparison.Ordinal);
    if (num1 < 0 || num2 < 0)
      throw new FormatException($"Unexpected DateTime format: \"{value}\"");
    long result;
    if (!long.TryParse(value.Substring(checked (num1 + 1), checked (num2 - num1 - 1)), out result))
      throw new FormatException($"Unexpected DateTime format: \"{value}\"");
    if (result < ODataResultTableV2.MinMilliseconds)
      return DateTime.MinValue;
    return result > ODataResultTableV2.MaxMilliseconds ? DateTime.MaxValue : ODataResultTableV2.InitDateTime.AddMilliseconds((double) result);
  }

  private static DateTime ParseAndReturnDateTimeOffset(string value)
  {
    int num1 = value.IndexOf("(", StringComparison.Ordinal);
    int num2 = value.IndexOf(")", StringComparison.Ordinal);
    if (num1 < 0 || num2 < 0)
      throw new FormatException($"Unexpected DateTimeOffset format: \"{value}\"");
    int num3 = value.IndexOf("+", StringComparison.Ordinal);
    bool flag = true;
    if (num3 < 0)
    {
      num3 = value.IndexOf("-", StringComparison.Ordinal);
      if (num3 < 0)
        throw new FormatException($"Datetimeoffset addition/subtraction operator not found: \"{value}\"");
      flag = false;
    }
    string s1 = value.Substring(checked (num1 + 1), checked (num3 - num1 - 1));
    long result1;
    if (!long.TryParse(s1, out result1))
      throw new FormatException($"Could not parse datetimeoffset miliseconds part: \"{s1}\"");
    DateTime returnDateTimeOffset = ODataResultTableV2.InitDateTime.AddMilliseconds((double) result1);
    string s2 = value.Substring(checked (num3 + 1), checked (num2 - num3 - 1));
    int result2;
    if (!int.TryParse(s2, out result2))
      throw new FormatException($"Could not parse datetimeoffset minutes part: \"{s2}\"");
    if (result2 != 0)
      returnDateTimeOffset = returnDateTimeOffset.AddMinutes(flag ? (double) result2 : (double) checked (result2 * -1));
    return returnDateTimeOffset;
  }

  protected override object? Convert(object? value, int columnIndex)
  {
    if (value == null)
      return (object) null;
    switch (this.definition.Service.SelectedFields[columnIndex].Type)
    {
      case EdmType.DateTime:
        return value is string str1 ? (object) ODataResultTableV2.ParseAndReturnDateTime(str1).ToString("yyyyMMdd") : throw new InvalidOperationException("Expected Date as string");
      case EdmType.DateTimeOffset:
        return value is string str2 ? (object) ODataResultTableV2.ParseAndReturnDateTimeOffset(str2).ToString("yyyyMMdd") : throw new InvalidOperationException("Expected Date as string");
      default:
        return base.Convert(value, columnIndex);
    }
  }

  public override IResultTable ShallowCopy()
  {
    return (IResultTable) new ODataResultTableV2(this.definition, (IList<object[]>) this.results);
  }
}
