// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.Definition
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using Theobald.Extractors.Common;
using Theobald.Extractors.Conversion;
using Theobald.Json;
using Theobald.Logging;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class Definition : 
  IODataExtractionDefinition,
  IExtractionDefinition,
  IJsonSerializable,
  IDeepCopyable,
  IUsageDataCollectable,
  IDeltaCapable
{
  public string RelativeServiceUrl { get; set; } = string.Empty;

  public Settings Settings { get; set; } = new Settings();

  public string TechnicalName { get; set; } = string.Empty;

  public string EntitySetName { get; set; } = string.Empty;

  public string Description { get; set; } = string.Empty;

  public IReadOnlyDictionary<string, Field> Fields { get; set; } = (IReadOnlyDictionary<string, Field>) new Dictionary<string, Field>();

  public bool SupportsChangeTracking { get; set; }

  public OdpODataParameterization Parameterization { get; }

  IComplexParameterizableObject IExtractionDefinition.Parameterization
  {
    get => (IComplexParameterizableObject) this.Parameterization;
  }

  public bool IsDeltaEnabled => this.Settings.TrackChanges;

  public Definition()
  {
    this.Parameterization = new OdpODataParameterization()
    {
      Definition = this
    };
  }

  public static List<Definition> FromServiceSchema(
    string serviceName,
    string description,
    string relativeServiceUrl,
    DataServiceSchema schema)
  {
    EntityContainer entityContainer = schema.DefaultContainer ?? throw new DefaultContainerMissing();
    string supportedFormats = entityContainer.SupportedFormats;
    if (supportedFormats == null || !supportedFormats.Contains("json"))
      throw new ContainerFormatException("json");
    List<Definition> definitionList = new List<Definition>(entityContainer.EntitySets.Count);
    foreach (KeyValuePair<string, EntitySet> entitySet1 in (IEnumerable<KeyValuePair<string, EntitySet>>) entityContainer.EntitySets)
    {
      if (Lookup.IsOdpRelatedEntitySetName(entitySet1.Key))
      {
        EntitySet entitySet2 = entitySet1.Value;
        Dictionary<string, Field> dictionary = new Dictionary<string, Field>(entitySet2.EntityType.Properties.Count);
        entitySet2 = entitySet1.Value;
        foreach (KeyValuePair<string, Property> property in (IEnumerable<KeyValuePair<string, Property>>) entitySet2.EntityType.Properties)
        {
          Field field1 = Field.FromProperty(property.Value);
          Field field2 = field1;
          entitySet2 = entitySet1.Value;
          int num = entitySet2.EntityType.Key.Contains(new NamedRef()
          {
            Name = property.Key
          }) ? 1 : 0;
          field2.IsPrimaryKey = num != 0;
          field1.IsSelected = true;
          dictionary.Add(property.Key, field1);
        }
        bool flag = false;
        entitySet2 = entitySet1.Value;
        IXmlAttributeValue xmlAttributeValue;
        if (entitySet2.AdditionalAttributes.TryGetValue("sap:change-tracking", out xmlAttributeValue))
          flag = xmlAttributeValue.AsBoolean();
        Definition definition = new Definition()
        {
          TechnicalName = serviceName,
          EntitySetName = entitySet1.Key,
          Description = description,
          RelativeServiceUrl = relativeServiceUrl,
          Fields = (IReadOnlyDictionary<string, Field>) dictionary,
          SupportsChangeTracking = flag
        };
        definitionList.Add(definition);
      }
    }
    return definitionList;
  }

  public IEnumerable<ResultColumn> NewResultColumns(ColumnNameConverter columnNameConverter)
  {
    foreach (KeyValuePair<string, Field> keyValuePair in this.Fields.Where<KeyValuePair<string, Field>>((Func<KeyValuePair<string, Field>, bool>) (f => f.Value.IsSelected)))
    {
      (ResultType resultType, int length) = TypeMappings.TranslateTypeInfo(keyValuePair.Value);
      yield return new ResultColumn(columnNameConverter, keyValuePair.Key, keyValuePair.Value.Description, this.TechnicalName, string.Empty, resultType, length, keyValuePair.Value.Scale.GetValueOrDefault(), keyValuePair.Value.IsPrimaryKey, false);
    }
  }

  public IExtractor NewExtractor(IODataConnection connection)
  {
    return (IExtractor) new Extractor((IOdpODataExtractionApi) new OdpODataExtractionApi((IODataClient) new ODataClient((IHttpClient) new HttpClientWrapper(new HttpClient()
    {
      Timeout = TimeSpan.FromHours(8.0)
    }), connection)), this);
  }

  public IResultTable RunPreview(IODataConnection connection, ILog logger)
  {
    return (IResultTable) new Extractor((IOdpODataExtractionApi) new OdpODataExtractionApi((IODataClient) new ODataClient((IHttpClient) new HttpClientWrapper(new HttpClient()), connection)), this).PreviewAsync(logger, CancellationToken.None).Result.ReadAsync().Result;
  }

  public bool ReadJson(JsonReader reader)
  {
    DefinitionSerialization definitionSerialization = new DefinitionSerialization();
    if (!definitionSerialization.ReadJson(reader))
      return false;
    definitionSerialization.ToDefinition(this);
    return true;
  }

  public void WriteJson(JsonWriter writer)
  {
    DefinitionSerialization.FromDefinition(this).WriteJson(writer);
  }

  public object ObjectCopyDeep()
  {
    Definition definition = new Definition()
    {
      RelativeServiceUrl = this.RelativeServiceUrl,
      TechnicalName = this.TechnicalName,
      EntitySetName = this.EntitySetName,
      Description = this.Description,
      Settings = this.Settings.CopyDeep<Settings>(),
      SupportsChangeTracking = this.SupportsChangeTracking,
      Fields = (IReadOnlyDictionary<string, Field>) this.Fields.CopyDeep<Field>()
    };
    ComplexRuntimeParameterSet runtimeParameters = this.Parameterization.CustomParameters.CopyDeep<ComplexRuntimeParameterSet>();
    definition.Parameterization.SetParameters(in runtimeParameters);
    return (object) definition;
  }

  public IEnumerable<string> CollectSapObjectNames()
  {
    if (!string.IsNullOrWhiteSpace(this.TechnicalName))
      yield return this.TechnicalName;
  }
}
