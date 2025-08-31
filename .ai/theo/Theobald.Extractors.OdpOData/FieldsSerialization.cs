// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.FieldsSerialization
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Linq;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

internal sealed class FieldsSerialization : JsonSerializableObject
{
  private Dictionary<string, FieldSerialization> fieldSerializations = new Dictionary<string, FieldSerialization>();

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    FieldSerialization fieldSerialization = reader.ReadObject<FieldSerialization>();
    this.fieldSerializations.Add(member, fieldSerialization);
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    foreach (KeyValuePair<string, FieldSerialization> fieldSerialization in this.fieldSerializations)
      writer.Write(fieldSerialization.Key, (IJsonSerializable) fieldSerialization.Value);
  }

  public static FieldsSerialization FromFields(IReadOnlyDictionary<string, Field> fields)
  {
    return new FieldsSerialization()
    {
      fieldSerializations = fields.ToDictionary<KeyValuePair<string, Field>, string, FieldSerialization>((Func<KeyValuePair<string, Field>, string>) (p => p.Key), (Func<KeyValuePair<string, Field>, FieldSerialization>) (p => FieldSerialization.FromField(p.Value)))
    };
  }

  public Dictionary<string, Field> ToFields()
  {
    return this.fieldSerializations.ToDictionary<KeyValuePair<string, FieldSerialization>, string, Field>((Func<KeyValuePair<string, FieldSerialization>, string>) (p => p.Key), (Func<KeyValuePair<string, FieldSerialization>, Field>) (p => p.Value.ToField()));
  }
}
