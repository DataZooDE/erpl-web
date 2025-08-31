// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.MultiValueFilter
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class MultiValueFilter : JsonSerializableObject, IFilter, IDeepCopyable
{
  public IList<FilterValue> Values { get; set; } = (IList<FilterValue>) new List<FilterValue>();

  public ListRuntimeParameter? RuntimeParameter { get; set; }

  public override void WriteJsonFields(JsonWriter writer)
  {
    writer.Write<FilterValue>("values", (IEnumerable<FilterValue>) this.Values);
    writer.Write("parameter", this.RuntimeParameter?.Name);
  }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    switch (member)
    {
      case "values":
        this.Values = reader.ReadObjects<FilterValue>();
        break;
      case "parameter":
        this.RuntimeParameter = new ListRuntimeParameter(reader.ReadString());
        break;
      default:
        base.ReadJsonProperty(reader, member);
        break;
    }
  }

  public object ObjectCopyDeep()
  {
    MultiValueFilter multiValueFilter1 = new MultiValueFilter();
    MultiValueFilter multiValueFilter2 = multiValueFilter1;
    IList<FilterValue> values = this.Values;
    int count = values.Count;
    List<FilterValue> list = new List<FilterValue>(count);
    CollectionsMarshal.SetCount<FilterValue>(list, count);
    Span<FilterValue> span = CollectionsMarshal.AsSpan<FilterValue>(list);
    int index = 0;
    foreach (FilterValue filterValue in (IEnumerable<FilterValue>) values)
    {
      span[index] = filterValue;
      ++index;
    }
    multiValueFilter2.Values = (IList<FilterValue>) list;
    MultiValueFilter multiValueFilter3 = multiValueFilter1;
    ListRuntimeParameter runtimeParameter1 = this.RuntimeParameter;
    ListRuntimeParameter runtimeParameter2 = runtimeParameter1 != null ? runtimeParameter1.CopyDeep<ListRuntimeParameter>() : (ListRuntimeParameter) null;
    multiValueFilter3.RuntimeParameter = runtimeParameter2;
    return (object) multiValueFilter1;
  }
}
