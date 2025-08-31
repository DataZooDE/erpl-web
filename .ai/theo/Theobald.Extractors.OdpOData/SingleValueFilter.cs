// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.SingleValueFilter
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class SingleValueFilter : JsonSerializableObject, IFilter, IDeepCopyable
{
  public FilterValue? Value { get; set; }

  public RuntimeParameter? RuntimeParameter { get; set; }

  public override void WriteJsonFields(JsonWriter writer)
  {
    writer.Write("value", (IJsonSerializable) this.Value);
    writer.Write("parameter", this.RuntimeParameter?.Name);
  }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    switch (member)
    {
      case "value":
        this.Value = reader.ReadObject<FilterValue>();
        break;
      case "parameter":
        this.RuntimeParameter = (RuntimeParameter) new ScalarRuntimeParameter(reader.ReadString(), RuntimeParameterType.String);
        break;
      default:
        base.ReadJsonProperty(reader, member);
        break;
    }
  }

  public object ObjectCopyDeep()
  {
    SingleValueFilter singleValueFilter = new SingleValueFilter();
    singleValueFilter.Value = this.Value;
    RuntimeParameter runtimeParameter = this.RuntimeParameter;
    singleValueFilter.RuntimeParameter = runtimeParameter != null ? runtimeParameter.CopyDeep<RuntimeParameter>() : (RuntimeParameter) null;
    return (object) singleValueFilter;
  }
}
