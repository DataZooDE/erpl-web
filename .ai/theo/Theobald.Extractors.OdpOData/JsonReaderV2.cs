// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.JsonReaderV2
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public class JsonReaderV2
{
  private const int MaxDecimalLength = 28;
  private readonly IReadOnlyList<KeyValuePair<string, Field>> fields;
  private readonly Dictionary<string, int> indexMap;

  public JsonReaderV2(IReadOnlyList<KeyValuePair<string, Field>> fields)
  {
    this.fields = fields;
    this.indexMap = new Dictionary<string, int>(this.fields.Count);
    int index = 0;
    while (index < this.fields.Count)
    {
      this.indexMap.Add(this.fields[index].Key, index);
      checked { ++index; }
    }
  }

  public JsonResultV2 Read(Stream stream)
  {
    using (StreamReader streamReader = new StreamReader(stream, Encoding.UTF8, false, 4096 /*0x1000*/, true))
    {
      JsonReader reader = new JsonReader((TextReader) streamReader);
      JsonResultStructureV2.ReadDataStart(reader);
      (List<object[]> results, string str) = JsonResultStructureV2.ReadPagedResults<object[]>(reader, new Func<JsonReader, object[]>(this.ReadSingleResult));
      return new JsonResultV2(this.fields, results)
      {
        NextUrl = str
      };
    }
  }

  private static DateTime? ReadDateTime(JsonReader reader)
  {
    string str = reader.ReadString();
    if (str == null)
      return new DateTime?();
    return str.StartsWith("/Date(") && str.EndsWith(")/") ? new DateTime?(new DateTime(1970, 1, 1).AddMilliseconds((double) long.Parse(str.Substring(6, checked (str.Length - 6 - 2))))) : throw new InvalidDataException($"Invalid DateTime format '{str}'");
  }

  private object?[] ReadSingleResult(JsonReader reader)
  {
    object[] objArray = new object[this.fields.Count];
    KeyValuePair<string, Field> field;
    while (true)
    {
      string key = reader.ReadName();
      if (key != null)
      {
        int index;
        if (!this.indexMap.TryGetValue(key, out index))
        {
          reader.Skip();
        }
        else
        {
          field = this.fields[index];
          object valueParsed = JsonReaderV2.GetValueParsed(reader, field.Value);
          if (valueParsed != null || field.Value.IsNullable)
            objArray[index] = valueParsed;
          else
            break;
        }
      }
      else
        goto label_7;
    }
    throw new NullValueInNonNullableProperty(field.Key);
label_7:
    return objArray;
  }

  private static object? GetValueParsed(JsonReader reader, Field field)
  {
    switch (field.Type)
    {
      case EdmType.Binary:
        string s1 = reader.ReadString();
        return s1 != null ? (object) Convert.FromBase64String(s1) : (object) null;
      case EdmType.Boolean:
        return (object) reader.ReadBooleanNullable();
      case EdmType.Byte:
        string s2 = reader.ReadString();
        return s2 != null ? (object) byte.Parse(s2, (IFormatProvider) CultureInfo.InvariantCulture) : (object) null;
      case EdmType.DateTime:
        return (object) JsonReaderV2.ReadDateTime(reader);
      case EdmType.DateTimeOffset:
        string input1 = reader.ReadString();
        return input1 != null ? (object) DateTimeOffset.ParseExact(input1, "yyyy-MM-ddTHH:mm:ssZ", (IFormatProvider) CultureInfo.InvariantCulture) : (object) null;
      case EdmType.Decimal:
        string str = reader.ReadString();
        if (str == null)
          return (object) null;
        if (str.Split('.')[0].Replace("-", "").Length > 28)
          throw new ArgumentOutOfRangeException(str, "A packed number exceeds the maximum supported integer places of 28");
        return str != null ? (object) Decimal.Parse(str, (IFormatProvider) CultureInfo.InvariantCulture) : (object) null;
      case EdmType.Double:
        string s3 = reader.ReadString();
        return s3 != null ? (object) double.Parse(s3, (IFormatProvider) CultureInfo.InvariantCulture) : (object) null;
      case EdmType.Single:
        string s4 = reader.ReadString();
        return s4 != null ? (object) float.Parse(s4, (IFormatProvider) CultureInfo.InvariantCulture) : (object) s4;
      case EdmType.Guid:
        string input2 = reader.ReadString();
        return input2 != null ? (object) Guid.Parse(input2) : (object) null;
      case EdmType.Int16:
        int? nullable = reader.ReadInt32Nullable();
        return (object) (nullable.HasValue ? new short?(checked ((short) nullable.GetValueOrDefault())) : new short?());
      case EdmType.Int32:
        return (object) reader.ReadInt32Nullable();
      case EdmType.Int64:
        string s5 = reader.ReadString();
        return s5 != null ? (object) long.Parse(s5, (IFormatProvider) CultureInfo.InvariantCulture) : (object) null;
      case EdmType.SByte:
        string s6 = reader.ReadString();
        return s6 != null ? (object) sbyte.Parse(s6, (IFormatProvider) CultureInfo.InvariantCulture) : (object) null;
      case EdmType.String:
        return (object) reader.ReadString();
      case EdmType.Time:
        string input3 = reader.ReadString();
        return input3 != null ? (object) TimeSpan.ParseExact(input3, "\\P\\Th\\Hmm\\M", (IFormatProvider) CultureInfo.InvariantCulture) : (object) null;
      default:
        throw new ArgumentOutOfRangeException();
    }
  }
}
