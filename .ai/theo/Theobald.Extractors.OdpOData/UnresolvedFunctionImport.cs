// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.UnresolvedFunctionImport
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct UnresolvedFunctionImport
{
  public required NamedRef ReturnType { get; init; }

  public static KeyValuePair<string, UnresolvedFunctionImport> FromXml(IXmlNode node)
  {
    XmlAttributeStash attributes = node.GetAttributes();
    string key = attributes.TakeAttributeRequired("Name").AsString();
    NamedRef namedRef = NamedRef.Parse(attributes.TakeAttributeRequired("ReturnType").AsString());
    UnresolvedFunctionImport unresolvedFunctionImport = new UnresolvedFunctionImport()
    {
      ReturnType = namedRef
    };
    return new KeyValuePair<string, UnresolvedFunctionImport>(key, unresolvedFunctionImport);
  }
}
