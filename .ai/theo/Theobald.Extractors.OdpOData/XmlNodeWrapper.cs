// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.XmlNodeWrapper
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public class XmlNodeWrapper(XmlNode node) : IXmlNode
{
  public string Name => node.Name;

  public int ChildNodeCount => node.ChildNodes.Count;

  public IEnumerable<IXmlNode> EnumerateChildNodes()
  {
    return (IEnumerable<IXmlNode>) node.ChildNodes.Cast<XmlNode>().Select<XmlNode, XmlNodeWrapper>((Func<XmlNode, XmlNodeWrapper>) (n => new XmlNodeWrapper(n)));
  }

  public XmlAttributeStash GetAttributes()
  {
    XmlAttributeCollection attributes = node.Attributes;
    return XmlAttributeStash.Create((IEnumerable<XmlAttribute>) ((attributes != null ? (object) attributes.Cast<XmlAttribute>() : (object) null) ?? (object) Array.Empty<XmlAttribute>()));
  }
}
