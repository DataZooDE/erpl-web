// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.FieldSerialization
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

internal sealed class FieldSerialization : JsonSerializableObject
{
  private EdmType? type;
  private int? fixedLength;
  private int? maxLength;
  private bool? isNullable;
  private int? precision;
  private int? scale;
  private string? description;
  private bool? isPrimaryKey;
  private bool? isSelected;
  private IFilter? filter;
  private FilterRestriction? filterRestriction;

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    if (member != null)
    {
      switch (member.Length)
      {
        case 4:
          if (member == "type")
          {
            this.type = new EdmType?(reader.ReadEnum<EdmType>());
            return;
          }
          break;
        case 5:
          if (member == "scale")
          {
            this.scale = new int?(reader.ReadInt32());
            return;
          }
          break;
        case 6:
          if (member == "filter")
          {
            FilterSerialization filterSerialization = new FilterSerialization();
            filterSerialization.ReadJson(reader);
            this.filter = filterSerialization.Filter;
            return;
          }
          break;
        case 9:
          switch (member[0])
          {
            case 'm':
              if (member == "maxLength")
              {
                this.maxLength = new int?(reader.ReadInt32());
                return;
              }
              break;
            case 'p':
              if (member == "precision")
              {
                this.precision = new int?(reader.ReadInt32());
                return;
              }
              break;
          }
          break;
        case 10:
          switch (member[2])
          {
            case 'N':
              if (member == "isNullable")
              {
                this.isNullable = new bool?(reader.ReadBoolean());
                return;
              }
              break;
            case 'S':
              if (member == "isSelected")
              {
                this.isSelected = new bool?(reader.ReadBoolean());
                return;
              }
              break;
          }
          break;
        case 11:
          switch (member[0])
          {
            case 'd':
              if (member == "description")
              {
                this.description = reader.ReadString();
                return;
              }
              break;
            case 'f':
              if (member == "fixedLength")
              {
                this.fixedLength = new int?(reader.ReadInt32());
                return;
              }
              break;
          }
          break;
        case 12:
          if (member == "isPrimaryKey")
          {
            this.isPrimaryKey = new bool?(reader.ReadBoolean());
            return;
          }
          break;
        case 17:
          if (member == "filterRestriction")
          {
            this.filterRestriction = new FilterRestriction?(reader.ReadEnum<FilterRestriction>());
            return;
          }
          break;
      }
    }
    base.ReadJsonProperty(reader, member);
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    writer.Write("type", (Enum) (ValueType) this.type);
    writer.Write("fixedLength", this.fixedLength);
    writer.Write("maxLength", this.maxLength);
    writer.Write("isNullable", this.isNullable);
    writer.Write("precision", this.precision);
    writer.Write("scale", this.scale);
    writer.Write("description", this.description);
    writer.Write("isPrimaryKey", this.isPrimaryKey);
    writer.Write("isSelected", this.isSelected);
    writer.Write("filterRestriction", (Enum) (ValueType) this.filterRestriction);
    IFilter filter = this.filter;
    if (filter == null)
      return;
    writer.Write("filter", (IJsonSerializable) new FilterSerialization(filter));
  }

  public Field ToField()
  {
    Field field = new Field();
    field.Type = this.type ?? throw new FieldMissingInSerializationException("type");
    field.FixedLength = this.fixedLength;
    field.MaxLength = this.maxLength;
    field.IsNullable = this.isNullable ?? throw new FieldMissingInSerializationException("isNullable");
    field.Precision = this.precision;
    field.Scale = this.scale;
    field.Description = this.description ?? string.Empty;
    field.IsPrimaryKey = this.isPrimaryKey.GetValueOrDefault();
    field.IsSelected = this.isSelected.GetValueOrDefault();
    field.FilterRestriction = this.filterRestriction ?? throw new FieldMissingInSerializationException("filterRestriction");
    field.Filter = this.filter;
    return field;
  }

  public static FieldSerialization FromField(in Field field)
  {
    return new FieldSerialization()
    {
      type = new EdmType?(field.Type),
      fixedLength = field.FixedLength,
      maxLength = field.MaxLength,
      isNullable = new bool?(field.IsNullable),
      precision = field.Precision,
      scale = field.Scale,
      description = field.Description,
      isPrimaryKey = new bool?(field.IsPrimaryKey),
      isSelected = new bool?(field.IsSelected),
      filterRestriction = new FilterRestriction?(field.FilterRestriction),
      filter = field.Filter
    };
  }
}
