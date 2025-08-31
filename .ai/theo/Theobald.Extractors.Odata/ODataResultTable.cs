// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ODataResultTable
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.Odata;

public abstract class ODataResultTable : IResultTable
{
  protected readonly ODataExtractionDefinition definition;
  protected readonly List<object?[]> results;

  public int ColumnsCount
  {
    get
    {
      return (this.definition.Service ?? throw new InvalidOperationException("Definition not correctly initialized")).SelectedFields.ToList<ServiceField>().Count;
    }
  }

  public int RowsCount => this.results.Count;

  public int? SelectedRowIndex { get; set; }

  protected ODataResultTable(ODataExtractionDefinition definition)
  {
    this.definition = definition;
    this.results = new List<object[]>();
  }

  protected ODataResultTable(ODataExtractionDefinition definition, IList<object?[]> results)
  {
    this.definition = definition;
    this.results = results.ToList<object[]>();
  }

  public void AddResults(IList<object?[]> results)
  {
    this.results.AddRange((IEnumerable<object[]>) results);
  }

  public object? GetValue(int columnIndex)
  {
    if (!this.SelectedRowIndex.HasValue)
      throw new InvalidOperationException("No SelectedRowIndex");
    return this.Convert(this.results[this.SelectedRowIndex.Value][columnIndex], columnIndex);
  }

  protected virtual object? Convert(object? value, int columnIndex)
  {
    if (value == null)
      return (object) null;
    switch (this.definition.Service.SelectedFields[columnIndex].Type)
    {
      case EdmType.Binary:
      case EdmType.Boolean:
      case EdmType.Byte:
      case EdmType.Decimal:
      case EdmType.Double:
      case EdmType.Guid:
      case EdmType.Int16:
      case EdmType.Int32:
      case EdmType.Int64:
      case EdmType.SByte:
      case EdmType.String:
      case EdmType.Time:
      case EdmType.TimeOfDay:
        return value;
      case EdmType.Date:
        if (!(value is string s))
          throw new InvalidOperationException("Expected Date as string");
        if (s.Length != 10)
          throw new FormatException("Edm.Date should have 10 characters: " + s);
        return DateTime.TryParseExact(s, "yyyy-MM-dd", (IFormatProvider) CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime _) ? (object) s.Replace("-", string.Empty) : throw new FormatException("could not parse Edm.Date: " + s);
      default:
        throw new NotSupportedException($"{this.definition.Service.Fields[columnIndex].Type} not supported");
    }
  }

  public static ODataResultTable New(ODataExtractionDefinition definition, IList<object?[]> results)
  {
    if (definition.Service == null)
      throw new InvalidOperationException("Service not correctly initialized");
    if (definition.Service.Fields == null)
      throw new InvalidOperationException("Service fields not correctly initialized");
    return definition.Service.Version != ODataVersion.V2 ? (ODataResultTable) new ODataResultTableV4(definition, results) : (ODataResultTable) new ODataResultTableV2(definition, results);
  }

  public abstract IResultTable ShallowCopy();
}
