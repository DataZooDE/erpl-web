// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.DataServiceSchema
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct DataServiceSchema
{
  public required IReadOnlyDictionary<string, EntityType> EntityTypes { get; init; }

  public required IReadOnlyDictionary<string, ComplexType> ComplexTypes { get; init; }

  public required IReadOnlyDictionary<string, EntityContainer> EntityContainers { get; init; }

  public EntityContainer? DefaultContainer { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public DataServiceSchema()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CEntityTypes\u003Ek__BackingField = (IReadOnlyDictionary<string, EntityType>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CComplexTypes\u003Ek__BackingField = (IReadOnlyDictionary<string, ComplexType>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CEntityContainers\u003Ek__BackingField = (IReadOnlyDictionary<string, EntityContainer>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CDefaultContainer\u003Ek__BackingField = new EntityContainer?();
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  public static DataServiceSchema Resolve(UnresolvedDataServiceSchema schema)
  {
    Dictionary<string, EntityType> resolvedEntityTypes = new Dictionary<string, EntityType>(schema.EntityTypes.Count);
    foreach (KeyValuePair<string, UnresolvedEntityType> entityType1 in (IEnumerable<KeyValuePair<string, UnresolvedEntityType>>) schema.EntityTypes)
    {
      EntityType entityType2 = EntityType.Resolve(entityType1.Value);
      resolvedEntityTypes.Add(entityType1.Key, entityType2);
    }
    Dictionary<string, EntityContainer> dictionary = new Dictionary<string, EntityContainer>(schema.EntityContainers.Count);
    EntityContainer? nullable = new EntityContainer?();
    foreach (KeyValuePair<string, UnresolvedEntityContainer> entityContainer1 in (IEnumerable<KeyValuePair<string, UnresolvedEntityContainer>>) schema.EntityContainers)
    {
      EntityContainer entityContainer2 = EntityContainer.Resolve(entityContainer1.Value, schema, (IReadOnlyDictionary<string, EntityType>) resolvedEntityTypes);
      dictionary.Add(entityContainer1.Key, entityContainer2);
      if (entityContainer1.Value.IsDefaultEntityContainer)
        nullable = new EntityContainer?(entityContainer2);
    }
    return new DataServiceSchema()
    {
      EntityTypes = (IReadOnlyDictionary<string, EntityType>) resolvedEntityTypes,
      ComplexTypes = schema.ComplexTypes,
      EntityContainers = (IReadOnlyDictionary<string, EntityContainer>) dictionary,
      DefaultContainer = nullable,
      AdditionalAttributes = schema.AdditionalAttributes
    };
  }
}
