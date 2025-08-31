// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.UnresolvedDataServiceSchema
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct UnresolvedDataServiceSchema
{
  public required string Namespace { get; init; }

  public required IReadOnlyDictionary<string, UnresolvedEntityType> EntityTypes { get; init; }

  public required IReadOnlyDictionary<string, ComplexType> ComplexTypes { get; init; }

  public IReadOnlyDictionary<string, UnresolvedEntityContainer> EntityContainers { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public UnresolvedDataServiceSchema()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CNamespace\u003Ek__BackingField = (string) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CEntityTypes\u003Ek__BackingField = (IReadOnlyDictionary<string, UnresolvedEntityType>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CComplexTypes\u003Ek__BackingField = (IReadOnlyDictionary<string, ComplexType>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CEntityContainers\u003Ek__BackingField = (IReadOnlyDictionary<string, UnresolvedEntityContainer>) new Dictionary<string, UnresolvedEntityContainer>();
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  public static UnresolvedDataServiceSchema FromXml(IXmlNode node)
  {
    XmlAttributeStash attributes = node.GetAttributes();
    Dictionary<string, UnresolvedEntityType> dictionary1 = new Dictionary<string, UnresolvedEntityType>(node.ChildNodeCount);
    Dictionary<string, ComplexType> dictionary2 = new Dictionary<string, ComplexType>(node.ChildNodeCount);
    Dictionary<string, UnresolvedEntityContainer> dictionary3 = new Dictionary<string, UnresolvedEntityContainer>(node.ChildNodeCount);
    foreach (IXmlNode enumerateChildNode in node.EnumerateChildNodes())
    {
      switch (enumerateChildNode.Name)
      {
        case "EntityType":
          KeyValuePair<string, UnresolvedEntityType> keyValuePair1 = UnresolvedEntityType.FromXml(enumerateChildNode);
          dictionary1.Add(keyValuePair1.Key, keyValuePair1.Value);
          continue;
        case "ComplexType":
          KeyValuePair<string, ComplexType> keyValuePair2 = ComplexType.FromXml(enumerateChildNode);
          dictionary2.Add(keyValuePair2.Key, keyValuePair2.Value);
          continue;
        case "EntityContainer":
          KeyValuePair<string, UnresolvedEntityContainer> keyValuePair3 = UnresolvedEntityContainer.FromXml(enumerateChildNode);
          dictionary3.Add(keyValuePair3.Key, keyValuePair3.Value);
          continue;
        default:
          continue;
      }
    }
    return new UnresolvedDataServiceSchema()
    {
      Namespace = attributes.TakeAttributeRequired("Namespace").AsString(),
      EntityTypes = (IReadOnlyDictionary<string, UnresolvedEntityType>) dictionary1,
      ComplexTypes = (IReadOnlyDictionary<string, ComplexType>) dictionary2,
      EntityContainers = (IReadOnlyDictionary<string, UnresolvedEntityContainer>) dictionary3,
      AdditionalAttributes = (IReadOnlyDictionary<string, IXmlAttributeValue>) attributes.Remaining()
    };
  }
}
