// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.FilterValue
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Globalization;
using Theobald.Common;
using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class FilterValue : JsonSerializableObject, IDeepCopyable
{
  public EdmType Type { get; private set; } = EdmType.String;

  public object? Value { get; private set; }

  public FilterValue()
  {
  }

  public FilterValue(EdmType type)
  {
    this.Type = type;
    object obj;
    switch (type)
    {
      case EdmType.Int32:
        obj = (object) 0;
        break;
      case EdmType.String:
        obj = (object) string.Empty;
        break;
      default:
        throw new NotSupportedException($"Type {type} is not yet supported");
    }
    this.Value = obj;
  }

  public FilterValue(EdmType type, object? value)
  {
    this.Type = type;
    this.SetValue(value);
  }

  private void ThrowIfIncorrectType<T>(object? value)
  {
    if (value != null && !(value is T))
      throw new InvalidOperationException($"Value has incorrect type for {this.Type}: {value.GetType()}");
  }

  public void SetValue(object? value)
  {
    switch (this.Type)
    {
      case EdmType.Int32:
        if (value != null && value is Number number)
        {
          this.ThrowIfIncorrectType<int?>((object) int.Parse(number.ToString()));
          break;
        }
        this.ThrowIfIncorrectType<int?>(value);
        break;
      case EdmType.String:
        this.ThrowIfIncorrectType<string>(value);
        break;
    }
    this.Value = value;
  }

  public static bool ValidateGuiString(EdmType type, string value)
  {
    switch (type)
    {
      case EdmType.Int32:
        return string.IsNullOrEmpty(value) || value.Equals("<null>") || int.TryParse(value, NumberStyles.Integer | NumberStyles.AllowThousands, (IFormatProvider) CultureInfo.InvariantCulture, out int _);
      case EdmType.String:
        return true;
      default:
        return false;
    }
  }

  public static FilterValue FromGuiString(EdmType type, string value)
  {
    switch (type)
    {
      case EdmType.Int32:
        return new FilterValue()
        {
          Type = type,
          Value = string.IsNullOrEmpty(value) || value.Equals("<null>") ? (object) null : (object) int.Parse(value, NumberStyles.Integer | NumberStyles.AllowThousands)
        };
      case EdmType.String:
        return new FilterValue()
        {
          Type = type,
          Value = (object) value
        };
      default:
        throw new NotSupportedException($"Type {type} is not yet supported");
    }
  }

  public string AsGuiString()
  {
    switch (this.Type)
    {
      case EdmType.Int32:
        int? nullable = this.AsInt32();
        ref int? local = ref nullable;
        return (local.HasValue ? local.GetValueOrDefault().ToString() : (string) null) ?? "";
      case EdmType.String:
        if (!(this.Value is string str))
          str = "";
        return str;
      default:
        throw new NotSupportedException($"Type {this.Type} is not yet supported");
    }
  }

  public string AsFilterString()
  {
    if (this.Value == null)
      return "null";
    switch (this.Type)
    {
      case EdmType.Int32:
        return this.AsInt32().ToString() ?? "";
      case EdmType.String:
        return $"'{this.AsString()}'";
      default:
        throw new NotSupportedException($"Type {this.Type} is not yet supported");
    }
  }

  private string? AsString()
  {
    if (this.Type != EdmType.String)
      throw new InvalidFilterValueTypeException(EdmType.String, this.Type);
    return (string) this.Value;
  }

  private int? AsInt32()
  {
    if (this.Type != EdmType.Int32)
      throw new InvalidFilterValueTypeException(EdmType.Int32, this.Type);
    if (this.Value == null)
      return new int?();
    if (this.Value is int num)
      return new int?(num);
    return this.Value is Number number ? new int?(int.Parse(number.ToString())) : throw new InvalidOperationException("Unexpected value type");
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    switch (this.Type)
    {
      case EdmType.Int32:
        int? nullable = this.AsInt32();
        if (nullable.HasValue)
        {
          int valueOrDefault = nullable.GetValueOrDefault();
          writer.Write("int", new int?(valueOrDefault));
          break;
        }
        writer.WriteNull("int");
        break;
      case EdmType.String:
        string str = this.AsString();
        if (str != null)
        {
          writer.Write("string", str);
          break;
        }
        writer.WriteNull("string");
        break;
    }
  }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    switch (member)
    {
      case "string":
        this.Type = EdmType.String;
        this.Value = (object) reader.ReadString();
        break;
      case "int":
        this.Type = EdmType.Int32;
        this.Value = (object) reader.ReadInt32Nullable();
        break;
      default:
        base.ReadJsonProperty(reader, member);
        break;
    }
  }

  public object ObjectCopyDeep()
  {
    return (object) new FilterValue()
    {
      Type = this.Type,
      Value = this.Value
    };
  }
}
