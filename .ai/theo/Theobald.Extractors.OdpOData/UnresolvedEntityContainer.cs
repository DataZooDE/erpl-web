// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.UnresolvedEntityContainer
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct UnresolvedEntityContainer
{
  public required IReadOnlyDictionary<string, UnresolvedEntitySet> EntitySets { get; init; }

  public required IReadOnlyDictionary<string, UnresolvedFunctionImport> FunctionImports { get; init; }

  public bool IsDefaultEntityContainer { get; init; }

  public string? SupportedFormats { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public UnresolvedEntityContainer()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CEntitySets\u003Ek__BackingField = (IReadOnlyDictionary<string, UnresolvedEntitySet>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CFunctionImports\u003Ek__BackingField = (IReadOnlyDictionary<string, UnresolvedFunctionImport>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CIsDefaultEntityContainer\u003Ek__BackingField = false;
    // ISSUE: reference to a compiler-generated field
    this.\u003CSupportedFormats\u003Ek__BackingField = (string) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  public static KeyValuePair<string, UnresolvedEntityContainer> FromXml(IXmlNode node)
  {
    XmlAttributeStash attributes = node.GetAttributes();
    string key = attributes.TakeAttributeRequired("Name").AsString();
    Dictionary<string, UnresolvedEntitySet> dictionary1 = new Dictionary<string, UnresolvedEntitySet>(node.ChildNodeCount);
    Dictionary<string, UnresolvedFunctionImport> dictionary2 = new Dictionary<string, UnresolvedFunctionImport>(node.ChildNodeCount);
    foreach (IXmlNode enumerateChildNode in node.EnumerateChildNodes())
    {
      switch (enumerateChildNode.Name)
      {
        case "EntitySet":
          KeyValuePair<string, UnresolvedEntitySet> keyValuePair1 = UnresolvedEntitySet.FromXml(enumerateChildNode);
          dictionary1.Add(keyValuePair1.Key, keyValuePair1.Value);
          continue;
        case "FunctionImport":
          KeyValuePair<string, UnresolvedFunctionImport> keyValuePair2 = UnresolvedFunctionImport.FromXml(enumerateChildNode);
          dictionary2.Add(keyValuePair2.Key, keyValuePair2.Value);
          continue;
        default:
          continue;
      }
    }
    UnresolvedEntityContainer unresolvedEntityContainer1 = new UnresolvedEntityContainer();
    unresolvedEntityContainer1.EntitySets = (IReadOnlyDictionary<string, UnresolvedEntitySet>) dictionary1;
    unresolvedEntityContainer1.FunctionImports = (IReadOnlyDictionary<string, UnresolvedFunctionImport>) dictionary2;
    ref UnresolvedEntityContainer local = ref unresolvedEntityContainer1;
    IXmlAttributeValue attributeOptional = attributes.TakeAttributeOptional("m:IsDefaultEntityContainer");
    int num = attributeOptional != null ? (attributeOptional.AsBoolean() ? 1 : 0) : 0;
    local.IsDefaultEntityContainer = num != 0;
    unresolvedEntityContainer1.SupportedFormats = attributes.TakeAttributeOptional("sap:supported-formats")?.AsString();
    unresolvedEntityContainer1.AdditionalAttributes = (IReadOnlyDictionary<string, IXmlAttributeValue>) attributes.Remaining();
    UnresolvedEntityContainer unresolvedEntityContainer2 = unresolvedEntityContainer1;
    return new KeyValuePair<string, UnresolvedEntityContainer>(key, unresolvedEntityContainer2);
  }
}
