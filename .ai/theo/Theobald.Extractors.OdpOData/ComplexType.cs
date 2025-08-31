// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.ComplexType
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct ComplexType
{
  public required IReadOnlyDictionary<string, Property> Properties { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public ComplexType()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CProperties\u003Ek__BackingField = (IReadOnlyDictionary<string, Property>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  private static Dictionary<string, Property> PropertiesFromXml(IXmlNode node)
  {
    Dictionary<string, Property> dictionary = new Dictionary<string, Property>(node.ChildNodeCount);
    foreach (IXmlNode enumerateChildNode in node.EnumerateChildNodes())
    {
      if (enumerateChildNode.Name == "Property")
      {
        KeyValuePair<string, Property> keyValuePair = Property.FromXml(enumerateChildNode);
        dictionary.Add(keyValuePair.Key, keyValuePair.Value);
      }
    }
    return dictionary;
  }

  public static KeyValuePair<string, ComplexType> FromXml(IXmlNode node)
  {
    XmlAttributeStash attributes = node.GetAttributes();
    return new KeyValuePair<string, ComplexType>(attributes.TakeAttributeRequired("Name").AsString(), new ComplexType()
    {
      Properties = (IReadOnlyDictionary<string, Property>) ComplexType.PropertiesFromXml(node),
      AdditionalAttributes = (IReadOnlyDictionary<string, IXmlAttributeValue>) attributes.Remaining()
    });
  }
}
