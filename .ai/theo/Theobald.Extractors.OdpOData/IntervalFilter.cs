// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.IntervalFilter
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class IntervalFilter : JsonSerializableObject, IFilter, IDeepCopyable
{
  public FilterValue? FromValue { get; set; }

  public FilterValue? ToValue { get; set; }

  public ScalarRuntimeParameter? FromParameter { get; set; }

  public ScalarRuntimeParameter? ToParameter { get; set; }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    switch (member)
    {
      case "fromValue":
        this.FromValue = reader.ReadObject<FilterValue>();
        break;
      case "toValue":
        this.ToValue = reader.ReadObject<FilterValue>();
        break;
      case "fromParameter":
        this.FromParameter = new ScalarRuntimeParameter(reader.ReadString(), RuntimeParameterType.String);
        break;
      case "toParameter":
        this.ToParameter = new ScalarRuntimeParameter(reader.ReadString(), RuntimeParameterType.String);
        break;
      default:
        base.ReadJsonProperty(reader, member);
        break;
    }
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    writer.Write("fromValue", (IJsonSerializable) this.FromValue);
    writer.Write("toValue", (IJsonSerializable) this.ToValue);
    writer.Write("fromParameter", this.FromParameter?.Name);
    writer.Write("toParameter", this.ToParameter?.Name);
  }

  public object ObjectCopyDeep()
  {
    IntervalFilter intervalFilter = new IntervalFilter();
    intervalFilter.FromValue = this.FromValue;
    intervalFilter.ToValue = this.ToValue;
    ScalarRuntimeParameter fromParameter = this.FromParameter;
    intervalFilter.FromParameter = fromParameter != null ? fromParameter.CopyDeep<ScalarRuntimeParameter>() : (ScalarRuntimeParameter) null;
    ScalarRuntimeParameter toParameter = this.ToParameter;
    intervalFilter.ToParameter = toParameter != null ? toParameter.CopyDeep<ScalarRuntimeParameter>() : (ScalarRuntimeParameter) null;
    return (object) intervalFilter;
  }
}
