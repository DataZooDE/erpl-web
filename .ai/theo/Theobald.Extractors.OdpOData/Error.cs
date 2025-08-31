// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.Error
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct Error
{
  public required string Code { get; init; }

  public required ErrorMessage Message { get; init; }

  public static async Task<Exception> FromErrorResponseAsync(HttpResponseMessage response)
  {
    try
    {
      return (Exception) new ODataErrorResponseException(response.StatusCode, Error.ReadFromJson(await response.Content.ReadAsStreamAsync()));
    }
    catch (JsonException ex)
    {
      return (Exception) new InvalidErrorException(response.StatusCode, (Exception) ex);
    }
  }

  public static Error ReadFromJson(Stream stream)
  {
    using (StreamReader streamReader = new StreamReader(stream, Encoding.UTF8, bufferSize: 4096 /*0x1000*/, leaveOpen: true))
    {
      JsonReader reader = new JsonReader((TextReader) streamReader);
      reader.ReadObjectStart();
      if (reader.ReadName() != "error")
        throw new JsonException("Expected property named `error`");
      string str = "NONE";
      ErrorMessage errorMessage = new ErrorMessage()
      {
        Language = "en-us",
        Value = "Detailed message is missing"
      };
      reader.ReadObjectStart();
      while (true)
      {
        switch (reader.ReadName())
        {
          case null:
            goto label_8;
          case "code":
            str = reader.ReadString();
            continue;
          case "message":
            errorMessage = ErrorMessage.FromJson(reader);
            continue;
          default:
            reader.Skip();
            continue;
        }
      }
label_8:
      return new Error()
      {
        Code = str,
        Message = errorMessage
      };
    }
  }
}
