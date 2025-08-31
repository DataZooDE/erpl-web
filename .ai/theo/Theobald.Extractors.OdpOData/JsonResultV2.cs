// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.JsonResultV2
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public class JsonResultV2(IReadOnlyList<KeyValuePair<string, Field>> fields, List<object?[]> results) : 
  IResultTable
{
  public IReadOnlyList<KeyValuePair<string, Field>> Fields { get; } = fields;

  public List<object?[]> Results { get; } = results;

  public int ColumnsCount => this.Fields.Count;

  public int RowsCount => this.Results.Count;

  public int? SelectedRowIndex { get; set; }

  public string? NextUrl { get; set; }

  public object? GetValue(int columnIndex)
  {
    return this.Results[this.SelectedRowIndex ?? throw new InvalidOperationException("No row selected")][columnIndex];
  }

  public IResultTable ShallowCopy() => (IResultTable) new JsonResultV2(this.Fields, this.Results);
}
