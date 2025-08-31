// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.EntityType
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;
using System.Collections.Immutable;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct EntityType
{
  public required IReadOnlyDictionary<string, Property> Properties { get; init; }

  public required ImmutableHashSet<NamedRef> Key { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public EntityType()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CProperties\u003Ek__BackingField = (IReadOnlyDictionary<string, Property>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CKey\u003Ek__BackingField = (ImmutableHashSet<NamedRef>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  public static EntityType Resolve(UnresolvedEntityType unresolved)
  {
    IReadOnlyDictionary<string, Property> properties = unresolved.Properties;
    ImmutableHashSet<NamedRef>.Builder builder = ImmutableHashSet.CreateBuilder<NamedRef>();
    foreach (NamedRef propertyRef in (IEnumerable<NamedRef>) unresolved.Key)
    {
      if (!properties.TryGetValue(propertyRef.Name, out Property _))
        throw new PropertyNotFoundException(propertyRef);
      builder.Add(propertyRef);
    }
    return new EntityType()
    {
      Properties = properties,
      Key = builder.ToImmutable(),
      AdditionalAttributes = unresolved.AdditionalAttributes
    };
  }
}
