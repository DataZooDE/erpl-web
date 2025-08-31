// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceJsonReaderV4
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Theobald.Extractors.Common;
using Theobald.Json;
using Theobald.SyntacticSugar;

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class ServiceJsonReaderV4 : ServiceJsonReader
{
  private readonly string baseUrl;

  public string? ServiceIdFilter { get; set; }

  public ServiceJsonReaderV4(string baseUrl) => this.baseUrl = baseUrl;

  private void ReadBegin(JsonReader reader)
  {
    reader.ReadObjectStart();
    while (reader.ReadName() != "value")
      reader.Skip();
  }

  public override IEnumerable<Service> ReadServices(Stream response, CancellationToken ct)
  {
    using (TextReader textReader = (TextReader) new StreamReader(response))
    {
      JsonReader reader3 = new JsonReader(textReader);
      this.ReadBegin(reader3);
      IList<Service> services = (IList<Service>) new List<Service>();
      if (!reader3.ReadArray((Func<JsonReader, bool>) (reader1 =>
      {
        ct.ThrowIfCancellationRequested();
        if (!reader1.ReadObjectStart())
          return false;
        while (true)
        {
          do
          {
            string str = reader1.ReadName();
            if (str != null)
            {
              if (str.Equals("DefaultSystem", StringComparison.OrdinalIgnoreCase))
                reader1.ReadObjectStart();
              else if (!str.Equals("Services", StringComparison.OrdinalIgnoreCase))
                goto label_8;
            }
            else
              goto label_9;
          }
          while (reader1.ReadArray((Func<JsonReader, bool>) (reader2 =>
          {
            ct.ThrowIfCancellationRequested();
            if (!reader2.ReadObjectStart())
              return false;
            string id = string.Empty;
            string name = string.Empty;
            string description = string.Empty;
            string serviceUrl = string.Empty;
            while (true)
            {
              do
              {
                switch (reader2.ReadName())
                {
                  case null:
                    goto label_12;
                  case "ServiceId":
                    id = reader2.ReadString();
                    name = id;
                    break;
                  case "Description":
                    description = reader2.ReadString();
                    break;
                  case "ServiceUrl":
                    serviceUrl = new Uri(this.baseUrl).GetLeftPart(UriPartial.Authority).TrimEnd('/') + reader2.ReadString();
                    break;
                  default:
                    reader2.Skip();
                    break;
                }
              }
              while (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(name) || string.IsNullOrEmpty(description) || string.IsNullOrEmpty(serviceUrl));
              if (StringExtensions.IsNullOrEmpty(this.ServiceIdFilter) || id.ToUpper().Contains(this.ServiceIdFilter.TrimEnd().ToUpper()) || description.ToUpper().Contains(this.ServiceIdFilter.TrimEnd().ToUpper()))
                services.Add(new Service(id, name, description, serviceUrl, ODataVersion.V4));
              id = string.Empty;
              name = string.Empty;
              description = string.Empty;
              serviceUrl = string.Empty;
            }
label_12:
            return true;
          })));
          break;
label_8:
          reader1.Skip();
        }
        throw new JsonException("Error reading services");
label_9:
        reader1.ReadObjectEnd();
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
      JsonReader reader1 = new JsonReader(textReader);
      this.ReadBegin(reader1);
      IList<string> endpoints = (IList<string>) new List<string>();
      if (!reader1.ReadArray((Func<JsonReader, bool>) (reader =>
      {
        ct.ThrowIfCancellationRequested();
        if (!reader.ReadObjectStart())
          return false;
        while (true)
        {
          string str = reader.ReadName();
          if (str != null)
          {
            if (str.Equals("url", StringComparison.OrdinalIgnoreCase))
              endpoints.Add(reader.ReadString());
            else
              reader.Skip();
          }
          else
            break;
        }
        return true;
      })))
        throw new JsonException("Error reading services");
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
      this.ReadBegin(reader);
      return this.ReadServiceDataNodes(reader, fields, ct);
    }
  }

  protected override Decimal? ReadDecimal(JsonReader reader)
  {
    try
    {
      return new Decimal?(reader.ReadDecimal());
    }
    catch (OverflowException ex)
    {
      throw new ArgumentOutOfRangeException("A packed number exceeds the maximum supported integer places of 28", (Exception) ex);
    }
  }

  protected override double ReadDouble(JsonReader reader) => reader.ReadDouble();
}
