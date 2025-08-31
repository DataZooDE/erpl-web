// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.XmlAttributeStash
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public class XmlAttributeStash
{
  private readonly Dictionary<string, IXmlAttributeValue> attributes;

  private XmlAttributeStash(Dictionary<string, IXmlAttributeValue> attributes)
  {
    this.attributes = attributes;
  }

  public static XmlAttributeStash Create(IEnumerable<XmlAttribute> attributes)
  {
    return new XmlAttributeStash(attributes.ToDictionary<XmlAttribute, string, IXmlAttributeValue>((Func<XmlAttribute, string>) (attribute => attribute.Name), (Func<XmlAttribute, IXmlAttributeValue>) (attribute => (IXmlAttributeValue) new XmlAttributeValue(attribute.InnerText))));
  }

  public IXmlAttributeValue? TakeAttributeOptional(string name)
  {
    IXmlAttributeValue attributeOptional;
    if (!this.attributes.TryGetValue(name, out attributeOptional))
      return (IXmlAttributeValue) null;
    this.attributes.Remove(name);
    return attributeOptional;
  }

  public IXmlAttributeValue TakeAttributeRequired(string name)
  {
    return this.TakeAttributeOptional(name) ?? throw new RequiredAttributeMissingException(name);
  }

  public Dictionary<string, IXmlAttributeValue> Remaining() => this.attributes;
}
