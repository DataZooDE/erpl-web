// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.LookupJsonResult
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

internal static class LookupJsonResult
{
  private static string ReadEntitySet(JsonReader reader)
  {
    string str = string.Empty;
    while (true)
    {
      switch (reader.ReadName())
      {
        case null:
          goto label_4;
        case "ID":
          str = reader.ReadString();
          continue;
        default:
          reader.Skip();
          continue;
      }
    }
label_4:
    return str;
  }

  private static ServiceCatalogEntry ReadCatalogEntry(JsonReader reader)
  {
    string str1 = (string) null;
    string str2 = (string) null;
    string str3 = (string) null;
    List<string> stringList = new List<string>();
    while (true)
    {
      switch (reader.ReadName())
      {
        case null:
          goto label_7;
        case "ID":
          str1 = reader.ReadString();
          continue;
        case "Description":
          str2 = reader.ReadString();
          continue;
        case "ServiceUrl":
          str3 = reader.ReadString();
          continue;
        case "EntitySets":
          // ISSUE: reference to a compiler-generated field
          // ISSUE: reference to a compiler-generated field
          stringList = JsonResultStructureV2.ReadResults<string>(reader, LookupJsonResult.\u003C\u003EO.\u003C0\u003E__ReadEntitySet ?? (LookupJsonResult.\u003C\u003EO.\u003C0\u003E__ReadEntitySet = new Func<JsonReader, string>(LookupJsonResult.ReadEntitySet)));
          continue;
        default:
          reader.Skip();
          continue;
      }
    }
label_7:
    ServiceCatalogEntry serviceCatalogEntry = new ServiceCatalogEntry();
    ref ServiceCatalogEntry local1 = ref serviceCatalogEntry;
    local1.Id = str1 ?? throw new InvalidDataException("Id is missing in catalog service");
    ref ServiceCatalogEntry local2 = ref serviceCatalogEntry;
    local2.Description = str2 ?? throw new InvalidDataException("Description is missing in catalog service");
    ref ServiceCatalogEntry local3 = ref serviceCatalogEntry;
    local3.ServiceUrl = str3 ?? throw new InvalidDataException("Service url missing in catalog service");
    serviceCatalogEntry.EntitySets = (IReadOnlyList<string>) stringList;
    return serviceCatalogEntry;
  }

  public static List<ServiceCatalogEntry> Read(Stream stream)
  {
    using (StreamReader streamReader = new StreamReader(stream, Encoding.UTF8, false, 4096 /*0x1000*/, true))
    {
      JsonReader reader = new JsonReader((TextReader) streamReader);
      JsonResultStructureV2.ReadDataStart(reader);
      // ISSUE: reference to a compiler-generated field
      // ISSUE: reference to a compiler-generated field
      return JsonResultStructureV2.ReadResults<ServiceCatalogEntry>(reader, LookupJsonResult.\u003C\u003EO.\u003C1\u003E__ReadCatalogEntry ?? (LookupJsonResult.\u003C\u003EO.\u003C1\u003E__ReadCatalogEntry = new Func<JsonReader, ServiceCatalogEntry>(LookupJsonResult.ReadCatalogEntry)));
    }
  }
}
