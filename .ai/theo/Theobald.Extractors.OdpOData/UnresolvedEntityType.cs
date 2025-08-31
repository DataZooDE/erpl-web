// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.UnresolvedEntityType
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct UnresolvedEntityType
{
  public required IReadOnlyDictionary<string, Property> Properties { get; init; }

  public required IReadOnlyList<NamedRef> Key { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public UnresolvedEntityType()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CProperties\u003Ek__BackingField = (IReadOnlyDictionary<string, Property>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CKey\u003Ek__BackingField = (IReadOnlyList<NamedRef>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  private static NamedRef PropertyReferenceFromXml(IXmlNode node)
  {
    return NamedRef.Parse(node.GetAttributes().TakeAttributeRequired("Name").AsString());
  }

  private static List<NamedRef> ReadKeyPropertyReferences(IXmlNode node)
  {
    List<NamedRef> namedRefList = new List<NamedRef>(node.ChildNodeCount);
    foreach (IXmlNode enumerateChildNode in node.EnumerateChildNodes())
    {
      if (enumerateChildNode.Name == "PropertyRef")
        namedRefList.Add(UnresolvedEntityType.PropertyReferenceFromXml(enumerateChildNode));
    }
    return namedRefList;
  }

  public static KeyValuePair<string, UnresolvedEntityType> FromXml(IXmlNode node)
  {
    List<NamedRef> namedRefList = new List<NamedRef>(0);
    Dictionary<string, Property> dictionary = new Dictionary<string, Property>(node.ChildNodeCount);
    foreach (IXmlNode enumerateChildNode in node.EnumerateChildNodes())
    {
      switch (enumerateChildNode.Name)
      {
        case "Key":
          namedRefList = UnresolvedEntityType.ReadKeyPropertyReferences(enumerateChildNode);
          continue;
        case "Property":
          KeyValuePair<string, Property> keyValuePair = Property.FromXml(enumerateChildNode);
          dictionary.Add(keyValuePair.Key, keyValuePair.Value);
          continue;
        default:
          continue;
      }
    }
    XmlAttributeStash attributes = node.GetAttributes();
    return new KeyValuePair<string, UnresolvedEntityType>(attributes.TakeAttributeRequired("Name").AsString(), new UnresolvedEntityType()
    {
      Key = (IReadOnlyList<NamedRef>) namedRefList,
      Properties = (IReadOnlyDictionary<string, Property>) dictionary,
      AdditionalAttributes = (IReadOnlyDictionary<string, IXmlAttributeValue>) attributes.Remaining()
    });
  }
}
