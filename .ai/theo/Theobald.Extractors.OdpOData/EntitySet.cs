// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.EntitySet
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct EntitySet
{
  public required EntityType EntityType { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public EntitySet()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CEntityType\u003Ek__BackingField = new EntityType();
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  public static EntitySet Resolve(
    UnresolvedEntitySet unresolved,
    UnresolvedDataServiceSchema schema,
    IReadOnlyDictionary<string, EntityType> resolvedEntityTypes)
  {
    NamedRef entityType1 = unresolved.EntityType;
    if (entityType1.Namespace != null && entityType1.Namespace != schema.Namespace)
      throw new DifferentNamespaceNotSupportedException(entityType1, schema.Namespace);
    EntityType entityType2;
    if (!resolvedEntityTypes.TryGetValue(entityType1.Name, out entityType2))
      throw new EntityTypeNotFoundException(entityType1);
    return new EntitySet()
    {
      EntityType = entityType2,
      AdditionalAttributes = unresolved.AdditionalAttributes
    };
  }
}
