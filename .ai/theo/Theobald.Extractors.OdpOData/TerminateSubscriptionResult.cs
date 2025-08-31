// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.TerminateSubscriptionResult
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.IO;
using System.Text;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct TerminateSubscriptionResult
{
  public required bool Success { get; init; }

  public static TerminateSubscriptionResult ReadFromJson(Stream stream, string functionName)
  {
    using (StreamReader streamReader = new StreamReader(stream, Encoding.UTF8, bufferSize: 4096 /*0x1000*/, leaveOpen: true))
    {
      JsonReader reader = new JsonReader((TextReader) streamReader);
      JsonResultStructureV2.ReadDataStart(reader);
      reader.ReadObjectStart();
      if (reader.ReadName() != functionName)
        throw new JsonException($"Expected property named `{functionName}`");
      reader.ReadObjectStart();
      bool flag = false;
      while (true)
      {
        switch (reader.ReadName())
        {
          case null:
            goto label_7;
          case "ResultFlag":
            flag = reader.ReadBoolean();
            continue;
          default:
            reader.Skip();
            continue;
        }
      }
label_7:
      return new TerminateSubscriptionResult()
      {
        Success = flag
      };
    }
  }
}
