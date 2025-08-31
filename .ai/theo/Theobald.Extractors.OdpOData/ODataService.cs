// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.ODataService
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct ODataService
{
  public required IReadOnlyList<DataServiceSchema> Schemata { get; init; }

  public static ODataService FromXmlStream(Stream stream)
  {
    NameTable nameTable = new NameTable();
    XmlNamespaceManager nsmgr = new XmlNamespaceManager((XmlNameTable) nameTable);
    nsmgr.AddNamespace("edmx", "http://schemas.microsoft.com/ado/2007/06/edmx");
    nsmgr.AddNamespace("m", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata");
    nsmgr.AddNamespace("sap", "http://www.sap.com/Protocols/SAPData");
    XmlDocument xmlDocument = new XmlDocument((XmlNameTable) nameTable);
    XmlReaderSettings settings = new XmlReaderSettings()
    {
      XmlResolver = (XmlResolver) null
    };
    using (XmlReader reader = XmlReader.Create(stream, settings))
    {
      xmlDocument.Load(reader);
      XmlNodeList source = xmlDocument.SelectNodes("/edmx:Edmx/edmx:DataServices", nsmgr);
      if (source == null || source.Count == 0)
        throw new ODataXmlRootElementMissingException();
      // ISSUE: reference to a compiler-generated field
      // ISSUE: reference to a compiler-generated field
      // ISSUE: reference to a compiler-generated field
      // ISSUE: reference to a compiler-generated field
      List<DataServiceSchema> list = source.Cast<XmlNode>().Select<XmlNode, XmlNodeWrapper>((Func<XmlNode, XmlNodeWrapper>) (n => new XmlNodeWrapper(n))).SelectMany<XmlNodeWrapper, DataServiceSchema>((Func<XmlNodeWrapper, IEnumerable<DataServiceSchema>>) (n => n.EnumerateChildNodes().Select<IXmlNode, UnresolvedDataServiceSchema>(ODataService.\u003C\u003EO.\u003C0\u003E__FromXml ?? (ODataService.\u003C\u003EO.\u003C0\u003E__FromXml = new Func<IXmlNode, UnresolvedDataServiceSchema>(UnresolvedDataServiceSchema.FromXml))).Select<UnresolvedDataServiceSchema, DataServiceSchema>(ODataService.\u003C\u003EO.\u003C1\u003E__Resolve ?? (ODataService.\u003C\u003EO.\u003C1\u003E__Resolve = new Func<UnresolvedDataServiceSchema, DataServiceSchema>(DataServiceSchema.Resolve))))).ToList<DataServiceSchema>();
      return new ODataService()
      {
        Schemata = (IReadOnlyList<DataServiceSchema>) list
      };
    }
  }
}
