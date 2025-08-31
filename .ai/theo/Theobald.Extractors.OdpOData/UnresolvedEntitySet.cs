// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.UnresolvedEntitySet
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct UnresolvedEntitySet
{
  public required NamedRef EntityType { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public UnresolvedEntitySet()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CEntityType\u003Ek__BackingField = new NamedRef();
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  public static KeyValuePair<string, UnresolvedEntitySet> FromXml(IXmlNode node)
  {
    XmlAttributeStash attributes = node.GetAttributes();
    string key = attributes.TakeAttributeRequired("Name").AsString();
    NamedRef namedRef = NamedRef.Parse(attributes.TakeAttributeRequired("EntityType").AsString());
    UnresolvedEntitySet unresolvedEntitySet = new UnresolvedEntitySet()
    {
      EntityType = namedRef,
      AdditionalAttributes = (IReadOnlyDictionary<string, IXmlAttributeValue>) attributes.Remaining()
    };
    return new KeyValuePair<string, UnresolvedEntitySet>(key, unresolvedEntitySet);
  }
}
