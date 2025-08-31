// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceXmlReader
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml;
using System.Xml.Linq;

#nullable enable
namespace Theobald.Extractors.Odata;

public abstract class ServiceXmlReader
{
  protected abstract string MetadataNamespace { get; }

  private static XmlReader GetNewXmlReader(Stream responseStream)
  {
    return XmlReader.Create(responseStream, new XmlReaderSettings()
    {
      Async = false
    });
  }

  public IEnumerable<ServiceField> ReadServiceMetadata(
    Stream response,
    string endpoint,
    CancellationToken ct)
  {
    using (XmlReader newXmlReader = ServiceXmlReader.GetNewXmlReader(response))
    {
      XDocument xdocument = XDocument.Load(newXmlReader);
      if (xdocument.Root == null)
        throw new InvalidOperationException("Xml Document root element not found");
      string endpointOnMetadata = (string) null;
      string metadataNamespace = this.MetadataNamespace;
      XElement xelement1 = (xdocument.Root.Descendants((XName) (metadataNamespace + "EntityContainer")).FirstOrDefault<XElement>() ?? throw new InvalidOperationException("Xml Document EntityContainer element not found")).Elements((XName) (metadataNamespace + "EntitySet")).FirstOrDefault<XElement>((Func<XElement, bool>) (element => element.HasAttributes && element.Attribute((XName) "Name") != null && element.Attribute((XName) "Name").Value.Equals(endpoint)));
      if (xelement1 != null && xelement1.HasAttributes)
        endpointOnMetadata = ((IEnumerable<string>) (xelement1.Attributes((XName) "EntityType").FirstOrDefault<XAttribute>() ?? throw new InvalidOperationException("Xml Document EntityType element not found")).Value.Split('.')).Last<string>();
      HashSet<ServiceField> serviceFieldSet = new HashSet<ServiceField>();
      XElement xelement2 = xdocument.Root.Descendants((XName) (metadataNamespace + "EntityType")).FirstOrDefault<XElement>((Func<XElement, bool>) (entity => entity.HasAttributes && entity.Attribute((XName) "Name") != null && entity.Attribute((XName) "Name").Value.Equals(endpointOnMetadata, StringComparison.OrdinalIgnoreCase)));
      if (xelement2 == null)
        throw new InvalidOperationException("Xml Document EntityType element not found");
      HashSet<string> stringSet = new HashSet<string>();
      XElement xelement3 = xelement2.Element((XName) (metadataNamespace + "Key"));
      if (xelement3 != null)
      {
        foreach (XElement element in xelement3.Elements((XName) (metadataNamespace + "PropertyRef")))
        {
          if (element.HasAttributes)
          {
            XAttribute xattribute = element.Attribute((XName) "Name");
            if (xattribute != null)
              stringSet.Add(xattribute.Value);
          }
        }
      }
      foreach (XElement element in xelement2.Elements((XName) (metadataNamespace + "Property")))
      {
        ct.ThrowIfCancellationRequested();
        XAttribute xattribute1 = element.Attribute((XName) "Type");
        if ((xattribute1 != null ? (xattribute1.Value.StartsWith("collection", StringComparison.OrdinalIgnoreCase) ? 1 : 0) : 1) == 0)
        {
          XAttribute xattribute2 = element.Attribute((XName) "Name");
          if (xattribute2 == null)
            throw new InvalidOperationException("OData service field without name");
          XAttribute xattribute3 = element.Attributes().FirstOrDefault<XAttribute>((Func<XAttribute, bool>) (attr => attr.Name.LocalName.Equals("label")));
          EdmType edmType;
          if (!ServiceField.EdmTypeDic.TryGetValue(xattribute1.Value, out edmType))
            throw new XmlException("unexpected service field type: " + xattribute1?.ToString());
          XAttribute xattribute4 = element.Attributes().FirstOrDefault<XAttribute>((Func<XAttribute, bool>) (attr => attr.Name == (XName) "MaxLength"));
          uint result1 = 0;
          if (xattribute4 != null && !uint.TryParse(xattribute4.Value, out result1))
            throw new InvalidOperationException("Unexpected MaxLength value: " + xattribute4?.ToString());
          this.AdjustFieldLength(edmType, ref result1);
          bool isPrimaryKey = stringSet.Contains(xattribute2.Value);
          XAttribute xattribute5 = element.Attributes().FirstOrDefault<XAttribute>((Func<XAttribute, bool>) (attr => attr.Name == (XName) "Precision"));
          XAttribute xattribute6 = element.Attributes().FirstOrDefault<XAttribute>((Func<XAttribute, bool>) (attr => attr.Name == (XName) "Scale"));
          if (xattribute5 == null)
          {
            serviceFieldSet.Add(new ServiceField(xattribute2.Value, xattribute3?.Value ?? string.Empty, edmType, result1, isPrimaryKey));
          }
          else
          {
            uint result2;
            if (!uint.TryParse(xattribute5.Value, out result2))
              throw new InvalidOperationException("Unexpected Precision value:" + xattribute5.Value);
            uint result3 = 0;
            if (xattribute6 != null)
              uint.TryParse(xattribute6.Value, out result3);
            serviceFieldSet.Add(new ServiceField(xattribute2.Value, xattribute3?.Value ?? string.Empty, edmType, result2, isPrimaryKey, result3, result3));
          }
        }
      }
      return (IEnumerable<ServiceField>) serviceFieldSet;
    }
  }

  private void AdjustFieldLength(EdmType fieldType, ref uint fieldLength)
  {
    switch (fieldType)
    {
      case EdmType.Boolean:
        fieldLength = 5U;
        break;
      case EdmType.DateTime:
      case EdmType.DateTimeOffset:
        fieldLength = 20U;
        break;
      case EdmType.Time:
      case EdmType.TimeOfDay:
        fieldLength = 8U;
        break;
      case EdmType.Date:
        fieldLength = 10U;
        break;
    }
  }
}
