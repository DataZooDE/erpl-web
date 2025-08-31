// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.JsonResultStructureV2
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.IO;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

internal static class JsonResultStructureV2
{
  public static void ReadDataStart(JsonReader reader)
  {
    reader.ReadObjectStart();
    if (reader.ReadName() != "d")
      throw new InvalidDataException("Expected root object named \"d\"");
  }

  private static List<TResult> ReadResultsArray<TResult>(
    JsonReader reader,
    Func<JsonReader, TResult> readElement)
  {
    List<TResult> results = new List<TResult>();
    if (!reader.ReadArray((Func<JsonReader, bool>) (r =>
    {
      if (!r.ReadObjectStart())
        return false;
      results.Add(readElement(r));
      return true;
    })))
      throw new JsonException("Expected results to be array");
    return results;
  }

  public static List<TResult> ReadResults<TResult>(
    JsonReader reader,
    Func<JsonReader, TResult> readElement)
  {
    reader.ReadObjectStart();
    List<TResult> resultList = new List<TResult>();
    while (true)
    {
      switch (reader.ReadName())
      {
        case null:
          goto label_4;
        case "results":
          resultList = JsonResultStructureV2.ReadResultsArray<TResult>(reader, readElement);
          continue;
        default:
          reader.Skip();
          continue;
      }
    }
label_4:
    return resultList;
  }

  public static (List<TResult>, string?) ReadPagedResults<TResult>(
    JsonReader reader,
    Func<JsonReader, TResult> readElement)
  {
    reader.ReadObjectStart();
    List<TResult> resultList = new List<TResult>();
    string str = (string) null;
    while (true)
    {
      switch (reader.ReadName())
      {
        case null:
          goto label_5;
        case "results":
          resultList = JsonResultStructureV2.ReadResultsArray<TResult>(reader, readElement);
          continue;
        case "__next":
          str = reader.ReadString();
          continue;
        default:
          reader.Skip();
          continue;
      }
    }
label_5:
    return (resultList, str);
  }
}
