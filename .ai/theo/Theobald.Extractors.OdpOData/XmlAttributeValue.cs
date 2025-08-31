// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.XmlAttributeValue
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

#nullable enable
namespace Theobald.Extractors.OdpOData;

public class XmlAttributeValue(string value) : IXmlAttributeValue
{
  public string AsString() => value;

  public int AsInt32() => int.Parse(value);

  public bool AsBoolean() => bool.Parse(value);
}
