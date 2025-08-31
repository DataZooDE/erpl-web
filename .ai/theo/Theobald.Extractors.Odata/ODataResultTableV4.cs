// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ODataResultTableV4
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.Globalization;
using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class ODataResultTableV4 : ODataResultTable
{
  internal ODataResultTableV4(ODataExtractionDefinition definition, IList<object?[]> results)
    : base(definition, results)
  {
  }

  private static DateTime ParseAndReturnDateTimeOffset(string value)
  {
    DateTime result;
    if (DateTime.TryParse(value, (IFormatProvider) CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out result))
      return result;
    throw new FormatException("could not parse Edm.DateTimeOffset: " + value);
  }

  protected override object? Convert(object? value, int columnIndex)
  {
    if (value == null)
      return (object) null;
    switch (this.definition.Service.SelectedFields[columnIndex].Type)
    {
      case EdmType.DateTime:
      case EdmType.DateTimeOffset:
        return value is string str ? (object) ODataResultTableV4.ParseAndReturnDateTimeOffset(str).ToString("yyyyMMdd") : throw new InvalidOperationException("Expected Date as string");
      default:
        return base.Convert(value, columnIndex);
    }
  }

  public override IResultTable ShallowCopy()
  {
    return (IResultTable) new ODataResultTableV4(this.definition, (IList<object[]>) this.results);
  }
}
