// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.Field
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class Field : IDeepCopyable
{
  public required EdmType Type { get; init; }

  public int? FixedLength { get; init; }

  public int? MaxLength { get; init; }

  public bool IsNullable { get; init; } = true;

  public int? Precision { get; init; }

  public int? Scale { get; init; }

  public string Description { get; init; } = string.Empty;

  public bool IsPrimaryKey { get; set; }

  public bool IsSelected { get; set; }

  public FilterRestriction FilterRestriction { get; init; }

  public IFilter? Filter { get; set; }

  public static Field FromProperty(Property property)
  {
    string str = string.Empty;
    IXmlAttributeValue xmlAttributeValue;
    if (property.AdditionalAttributes.TryGetValue("sap:label", out xmlAttributeValue))
      str = xmlAttributeValue.AsString();
    return new Field()
    {
      Type = property.Type,
      FixedLength = property.FixedLength,
      MaxLength = property.MaxLength,
      IsNullable = property.IsNullable,
      Precision = property.Precision,
      Scale = property.Scale,
      Description = str,
      FilterRestriction = Field.GetFilterRestriction(property)
    };
  }

  private static FilterRestriction GetFilterRestriction(Property property)
  {
    bool flag = true;
    IXmlAttributeValue xmlAttributeValue;
    if (property.AdditionalAttributes.TryGetValue("sap:filterable", out xmlAttributeValue))
      flag = xmlAttributeValue.AsBoolean();
    FilterRestriction filterRestriction1;
    if (flag)
    {
      if (!property.AdditionalAttributes.TryGetValue("sap:filter-restriction", out xmlAttributeValue))
        return FilterRestriction.Unrestricted;
      string str = xmlAttributeValue.AsString();
      FilterRestriction filterRestriction2;
      switch (str)
      {
        case "single-value":
          filterRestriction2 = FilterRestriction.SingleValue;
          break;
        case "multi-value":
          filterRestriction2 = FilterRestriction.MultiValue;
          break;
        case "interval":
          filterRestriction2 = FilterRestriction.Interval;
          break;
        default:
          throw new UnsupportedFilterRestrictionException(str);
      }
      filterRestriction1 = filterRestriction2;
    }
    else
      filterRestriction1 = FilterRestriction.Disallowed;
    return filterRestriction1;
  }

  public object ObjectCopyDeep()
  {
    Field field = new Field();
    field.Type = this.Type;
    field.FixedLength = this.FixedLength;
    field.MaxLength = this.MaxLength;
    field.IsNullable = this.IsNullable;
    field.Precision = this.Precision;
    field.Scale = this.Scale;
    field.Description = this.Description;
    field.IsPrimaryKey = this.IsPrimaryKey;
    field.IsSelected = this.IsSelected;
    field.FilterRestriction = this.FilterRestriction;
    IFilter filter = this.Filter;
    field.Filter = filter != null ? filter.CopyDeep<IFilter>() : (IFilter) null;
    return (object) field;
  }
}
