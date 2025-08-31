// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.Property
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct Property
{
  public required EdmType Type { get; init; }

  public string? Collation { get; init; }

  public string? Default { get; init; }

  public int? FixedLength { get; init; }

  public int? MaxLength { get; init; }

  public bool IsNullable { get; init; }

  public int? Precision { get; init; }

  public int? Scale { get; init; }

  public bool Unicode { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public Property()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CType\u003Ek__BackingField = EdmType.Binary;
    // ISSUE: reference to a compiler-generated field
    this.\u003CCollation\u003Ek__BackingField = (string) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CDefault\u003Ek__BackingField = (string) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CFixedLength\u003Ek__BackingField = new int?();
    // ISSUE: reference to a compiler-generated field
    this.\u003CMaxLength\u003Ek__BackingField = new int?();
    // ISSUE: reference to a compiler-generated field
    this.\u003CPrecision\u003Ek__BackingField = new int?();
    // ISSUE: reference to a compiler-generated field
    this.\u003CScale\u003Ek__BackingField = new int?();
    // ISSUE: reference to a compiler-generated field
    this.\u003CUnicode\u003Ek__BackingField = false;
    // ISSUE: reference to a compiler-generated field
    this.\u003CIsNullable\u003Ek__BackingField = true;
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  public static KeyValuePair<string, Property> FromXml(IXmlNode node)
  {
    XmlAttributeStash attributes = node.GetAttributes();
    string str = attributes.TakeAttributeRequired("Type").AsString();
    string typeName = str;
    if (str.StartsWith("Edm."))
      str = str.Substring(4);
    EdmType result;
    if (!Enum.TryParse<EdmType>(str, out result))
      throw new PropertyTypeNotSupportedException(typeName);
    string key = attributes.TakeAttributeRequired("Name").AsString();
    Property property1 = new Property();
    property1.Type = result;
    property1.Collation = attributes.TakeAttributeOptional("Collation")?.AsString();
    property1.Default = attributes.TakeAttributeOptional("Default")?.AsString();
    property1.FixedLength = attributes.TakeAttributeOptional("FixedLength")?.AsInt32();
    property1.MaxLength = attributes.TakeAttributeOptional("MaxLength")?.AsInt32();
    ref Property local1 = ref property1;
    IXmlAttributeValue attributeOptional1 = attributes.TakeAttributeOptional("Nullable");
    int num1 = attributeOptional1 != null ? (attributeOptional1.AsBoolean() ? 1 : 0) : 1;
    local1.IsNullable = num1 != 0;
    property1.Precision = attributes.TakeAttributeOptional("Precision")?.AsInt32();
    property1.Scale = attributes.TakeAttributeOptional("Scale")?.AsInt32();
    ref Property local2 = ref property1;
    IXmlAttributeValue attributeOptional2 = attributes.TakeAttributeOptional("Unicode");
    int num2 = attributeOptional2 != null ? (attributeOptional2.AsBoolean() ? 1 : 0) : 0;
    local2.Unicode = num2 != 0;
    property1.AdditionalAttributes = (IReadOnlyDictionary<string, IXmlAttributeValue>) attributes.Remaining();
    Property property2 = property1;
    return new KeyValuePair<string, Property>(key, property2);
  }
}
