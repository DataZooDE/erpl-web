// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.FilterSerialization
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class FilterSerialization : JsonSerializableObject
{
  public IFilter? Filter { get; private set; }

  public FilterSerialization()
  {
  }

  public FilterSerialization(IFilter filter) => this.Filter = filter;

  public override void WriteJsonFields(JsonWriter writer)
  {
    switch (this.Filter)
    {
      case SingleValueFilter singleValueFilter:
        writer.Write("single", (IJsonSerializable) singleValueFilter);
        break;
      case MultiValueFilter multiValueFilter:
        writer.Write("multiple", (IJsonSerializable) multiValueFilter);
        break;
      case IntervalFilter intervalFilter:
        writer.Write("interval", (IJsonSerializable) intervalFilter);
        break;
    }
  }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    switch (member)
    {
      case "single":
        SingleValueFilter singleValueFilter = new SingleValueFilter();
        singleValueFilter.ReadJson(reader);
        this.Filter = (IFilter) singleValueFilter;
        break;
      case "multiple":
        MultiValueFilter multiValueFilter = new MultiValueFilter();
        multiValueFilter.ReadJson(reader);
        this.Filter = (IFilter) multiValueFilter;
        break;
      case "interval":
        IntervalFilter intervalFilter = new IntervalFilter();
        intervalFilter.ReadJson(reader);
        this.Filter = (IFilter) intervalFilter;
        break;
      default:
        base.ReadJsonProperty(reader, member);
        break;
    }
  }
}
