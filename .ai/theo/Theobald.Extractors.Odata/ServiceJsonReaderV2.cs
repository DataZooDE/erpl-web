// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceJsonReaderV2
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class ServiceJsonReaderV2 : ServiceJsonReader
{
  private const int MaxDecimalLength = 28;

  private static void ReadBegin(JsonReader reader)
  {
    reader.ReadObjectStart();
    string str = reader.ReadName();
    if (!str.Equals("d", StringComparison.OrdinalIgnoreCase))
      throw new JsonException("unexpected token " + str);
    reader.ReadObjectStart();
  }

  public override IEnumerable<Service> ReadServices(Stream response, CancellationToken ct)
  {
    using (TextReader textReader = (TextReader) new StreamReader(response))
    {
      JsonReader reader1 = new JsonReader(textReader);
      ServiceJsonReaderV2.ReadBegin(reader1);
      string str1 = reader1.ReadName();
      if (!str1.Equals("results", StringComparison.OrdinalIgnoreCase))
        throw new JsonException("unexpected token " + str1);
      IList<Service> services = (IList<Service>) new List<Service>();
      if (!reader1.ReadArray((Func<JsonReader, bool>) (reader =>
      {
        ct.ThrowIfCancellationRequested();
        if (!reader.ReadObjectStart())
          return false;
        string id = string.Empty;
        string name = string.Empty;
        string description = string.Empty;
        string str2 = string.Empty;
        string serviceUrl = string.Empty;
        while (true)
        {
          do
          {
            switch (reader.ReadName())
            {
              case null:
                goto label_12;
              case "ID":
                id = reader.ReadString();
                break;
              case "Title":
                name = reader.ReadString();
                break;
              case "Description":
                description = reader.ReadString();
                break;
              case "MetadataUrl":
                str2 = reader.ReadString();
                break;
              case "ServiceUrl":
                serviceUrl = reader.ReadString();
                break;
              default:
                reader.Skip();
                break;
            }
          }
          while (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(name) || string.IsNullOrEmpty(description) || string.IsNullOrEmpty(str2) || string.IsNullOrEmpty(serviceUrl));
          services.Add(new Service(id, name, description, serviceUrl, ODataVersion.V2));
          id = string.Empty;
          name = string.Empty;
          description = string.Empty;
          str2 = string.Empty;
          serviceUrl = string.Empty;
        }
label_12:
        return true;
      })))
        throw new JsonException("Error reading services");
      return (IEnumerable<Service>) services;
    }
  }

  public override IEnumerable<string> ReadServiceEntities(Stream response, CancellationToken ct)
  {
    using (TextReader textReader = (TextReader) new StreamReader(response))
    {
      JsonReader reader = new JsonReader(textReader);
      ServiceJsonReaderV2.ReadBegin(reader);
      IList<string> endpoints = (IList<string>) new List<string>();
      string str = reader.ReadName();
      if (!str.Equals("EntitySets", StringComparison.OrdinalIgnoreCase))
        throw new JsonException("unexpected token " + str);
      reader.ReadStrings((Action<string>) (endpoint => endpoints.Add(endpoint)));
      return (IEnumerable<string>) endpoints;
    }
  }

  public override IList<object?[]> ReadServiceData(
    Stream response,
    IList<ServiceField> fields,
    CancellationToken ct)
  {
    using (TextReader textReader = (TextReader) new StreamReader(response))
    {
      JsonReader reader = new JsonReader(textReader);
      ServiceJsonReaderV2.ReadBegin(reader);
      if (reader.ReadName() == "results")
        return this.ReadServiceDataNodes(reader, fields, ct);
      throw new InvalidOperationException("expected ´results´ on json response");
    }
  }

  protected override Decimal? ReadDecimal(JsonReader reader)
  {
    string str = reader.ReadString();
    if (str == null)
      return new Decimal?();
    return str.Split('.')[0].Replace("-", "").Length <= 28 ? new Decimal?(Decimal.Parse(str)) : throw new ArgumentOutOfRangeException(str, "A packed number exceeds the maximum supported integer places of 28");
  }

  protected override double ReadDouble(JsonReader reader)
  {
    return double.Parse(reader.ReadString(), NumberStyles.Any);
  }
}
