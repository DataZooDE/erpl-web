// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceJsonReader
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Theobald.Json;
using Theobald.SyntacticSugar;

#nullable enable
namespace Theobald.Extractors.Odata;

public abstract class ServiceJsonReader
{
  protected const string DecimalOverflowMessage = "A packed number exceeds the maximum supported integer places of 28";

  public abstract IEnumerable<Service> ReadServices(Stream response, CancellationToken ct);

  public abstract IEnumerable<string> ReadServiceEntities(Stream response, CancellationToken ct);

  public abstract IList<object?[]> ReadServiceData(
    Stream response,
    IList<ServiceField> fields,
    CancellationToken ct);

  protected abstract Decimal? ReadDecimal(JsonReader reader);

  protected abstract double ReadDouble(JsonReader reader);

  protected IList<object?[]> ReadServiceDataNodes(
    JsonReader reader1,
    IList<ServiceField> fields,
    CancellationToken ct)
  {
    List<object[]> results = new List<object[]>();
    reader1.ReadArray((Func<JsonReader, bool>) (reader2 =>
    {
      ct.ThrowIfCancellationRequested();
      if (!reader2.ReadObjectStart())
        return false;
      int index = 0;
      object[] objArray = new object[fields.Count];
      try
      {
        ServiceField serviceField;
        while (true)
        {
          string name = reader2.ReadName();
          if (name != null)
          {
            serviceField = fields.FirstOrDefault<ServiceField>((Func<ServiceField, bool>) (field => field.Name.Equals(name, StringComparison.OrdinalIgnoreCase)));
            if (serviceField == null || StringExtensions.IsNullOrEmpty(serviceField.Name))
            {
              reader2.Skip();
            }
            else
            {
              switch (serviceField.Type)
              {
                case EdmType.Binary:
                  string s = reader2.ReadString();
                  objArray[index] = s == null ? (object) (byte[]) null : (object) Convert.FromBase64String(s);
                  break;
                case EdmType.Boolean:
                  bool flag = reader2.ReadBoolean();
                  objArray[index] = (object) flag.ToString().ToUpper();
                  break;
                case EdmType.Byte:
                  objArray[index] = (object) checked ((byte) reader2.ReadUInt16());
                  break;
                case EdmType.DateTime:
                  objArray[index] = (object) reader2.ReadString();
                  break;
                case EdmType.DateTimeOffset:
                  objArray[index] = (object) reader2.ReadString();
                  break;
                case EdmType.Decimal:
                  objArray[index] = (object) this.ReadDecimal(reader2);
                  break;
                case EdmType.Double:
                  objArray[index] = (object) this.ReadDouble(reader2);
                  break;
                case EdmType.Guid:
                case EdmType.String:
                  objArray[index] = (object) reader2.ReadString();
                  break;
                case EdmType.Int16:
                  objArray[index] = (object) checked ((short) reader2.ReadInt32());
                  break;
                case EdmType.Int32:
                  objArray[index] = (object) reader2.ReadInt32();
                  break;
                case EdmType.Int64:
                  objArray[index] = (object) reader2.ReadInt64();
                  break;
                case EdmType.SByte:
                  objArray[index] = (object) checked ((sbyte) reader2.ReadInt32());
                  break;
                case EdmType.Time:
                  objArray[index] = (object) reader2.ReadString();
                  break;
                case EdmType.Date:
                  objArray[index] = (object) reader2.ReadString();
                  break;
                case EdmType.TimeOfDay:
                  objArray[index] = (object) reader2.ReadString();
                  break;
                default:
                  goto label_22;
              }
              checked { ++index; }
            }
          }
          else
            goto label_24;
        }
label_22:
        throw new JsonException("type not handled: " + serviceField.Type.ToString());
label_24:
        results.Add(objArray);
      }
      catch (Exception ex)
      {
        throw new InvalidOperationException($"could not write row {results.Count} column {fields[index].Name}", ex);
      }
      return true;
    }));
    return (IList<object[]>) results;
  }
}
