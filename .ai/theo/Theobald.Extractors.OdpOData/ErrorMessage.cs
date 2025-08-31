// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.ErrorMessage
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct ErrorMessage
{
  public required string Language { get; init; }

  public required string Value { get; init; }

  public static ErrorMessage FromJson(JsonReader reader)
  {
    reader.ReadObjectStart();
    string str1 = (string) null;
    string str2 = (string) null;
    while (true)
    {
      switch (reader.ReadName())
      {
        case null:
          goto label_5;
        case "lang":
          str1 = reader.ReadString();
          continue;
        case "value":
          str2 = reader.ReadString();
          continue;
        default:
          reader.Skip();
          continue;
      }
    }
label_5:
    return new ErrorMessage()
    {
      Language = str1 ?? string.Empty,
      Value = str2 ?? string.Empty
    };
  }
}
