// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.DefinitionSerialization
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;
using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

internal sealed class DefinitionSerialization : JsonSerializableObject
{
  private string? entitySetName;
  private string? name;
  private string? description;
  private FieldsSerialization? fields;
  private bool? supportsChangeTracking;
  private string? relativeServiceUrl;
  private Settings? settings;
  private ComplexRuntimeParameterSet? customParameters;

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    if (member != null)
    {
      switch (member.Length)
      {
        case 4:
          if (member == "name")
          {
            this.name = reader.ReadString();
            return;
          }
          break;
        case 6:
          if (member == "fields")
          {
            this.fields = reader.ReadObject<FieldsSerialization>();
            return;
          }
          break;
        case 8:
          if (member == "settings")
          {
            this.settings = reader.ReadObject<Settings>();
            return;
          }
          break;
        case 10:
          if (member == "parameters")
          {
            this.customParameters = reader.ReadObject<ComplexRuntimeParameterSet>();
            return;
          }
          break;
        case 11:
          if (member == "description")
          {
            this.description = reader.ReadString();
            return;
          }
          break;
        case 13:
          if (member == "entitySetName")
          {
            this.entitySetName = reader.ReadString();
            return;
          }
          break;
        case 18:
          if (member == "relativeServiceUrl")
          {
            this.relativeServiceUrl = reader.ReadString();
            return;
          }
          break;
        case 22:
          if (member == "supportsChangeTracking")
          {
            this.supportsChangeTracking = new bool?(reader.ReadBoolean());
            return;
          }
          break;
      }
    }
    base.ReadJsonProperty(reader, member);
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    writer.Write("name", this.name);
    writer.Write("entitySetName", this.entitySetName);
    writer.Write("description", this.description);
    writer.Write("relativeServiceUrl", this.relativeServiceUrl);
    writer.Write("fields", (IJsonSerializable) this.fields);
    writer.Write("supportsChangeTracking", this.supportsChangeTracking);
    writer.Write("settings", (IJsonSerializable) this.settings);
    writer.Write("parameters", (IJsonSerializable) this.customParameters);
  }

  public void ToDefinition(Definition instance)
  {
    Definition definition1 = instance;
    definition1.TechnicalName = this.name ?? throw new FieldMissingInSerializationException("name");
    Definition definition2 = instance;
    definition2.EntitySetName = this.entitySetName ?? throw new FieldMissingInSerializationException("entitySetName");
    Definition definition3 = instance;
    definition3.Description = this.description ?? throw new FieldMissingInSerializationException("description");
    Definition definition4 = instance;
    definition4.RelativeServiceUrl = this.relativeServiceUrl ?? throw new FieldMissingInSerializationException("relativeServiceUrl");
    Definition definition5 = instance;
    definition5.Fields = (IReadOnlyDictionary<string, Field>) (this.fields?.ToFields() ?? throw new FieldMissingInSerializationException("fields"));
    Definition definition6 = instance;
    bool? supportsChangeTracking = this.supportsChangeTracking;
    if (!supportsChangeTracking.HasValue)
      throw new FieldMissingInSerializationException("supportsChangeTracking");
    int num = supportsChangeTracking.GetValueOrDefault() ? 1 : 0;
    definition6.SupportsChangeTracking = num != 0;
    Definition definition7 = instance;
    definition7.Settings = this.settings ?? throw new FieldMissingInSerializationException("settings");
    OdpODataParameterization parameterization = instance.Parameterization;
    ComplexRuntimeParameterSet runtimeParameterSet = this.customParameters ?? new ComplexRuntimeParameterSet();
    ref ComplexRuntimeParameterSet local = ref runtimeParameterSet;
    parameterization.SetParameters(in local);
  }

  public static DefinitionSerialization FromDefinition(in Definition definition)
  {
    return new DefinitionSerialization()
    {
      name = definition.TechnicalName,
      entitySetName = definition.EntitySetName,
      description = definition.Description,
      relativeServiceUrl = definition.RelativeServiceUrl,
      fields = FieldsSerialization.FromFields(definition.Fields),
      supportsChangeTracking = new bool?(definition.SupportsChangeTracking),
      settings = definition.Settings,
      customParameters = definition.Parameterization.CustomParameters
    };
  }
}
