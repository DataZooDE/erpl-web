// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.EntityContainer
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct EntityContainer
{
  public required IReadOnlyDictionary<string, EntitySet> EntitySets { get; init; }

  public required IReadOnlyDictionary<string, FunctionImport> FunctionImports { get; init; }

  public bool IsDefaultEntityContainer { get; init; }

  public string? SupportedFormats { get; init; }

  public IReadOnlyDictionary<string, IXmlAttributeValue> AdditionalAttributes { get; init; }

  public EntityContainer()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CEntitySets\u003Ek__BackingField = (IReadOnlyDictionary<string, EntitySet>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CFunctionImports\u003Ek__BackingField = (IReadOnlyDictionary<string, FunctionImport>) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CIsDefaultEntityContainer\u003Ek__BackingField = false;
    // ISSUE: reference to a compiler-generated field
    this.\u003CSupportedFormats\u003Ek__BackingField = (string) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CAdditionalAttributes\u003Ek__BackingField = (IReadOnlyDictionary<string, IXmlAttributeValue>) new Dictionary<string, IXmlAttributeValue>();
  }

  public static EntityContainer Resolve(
    UnresolvedEntityContainer unresolved,
    UnresolvedDataServiceSchema schema,
    IReadOnlyDictionary<string, EntityType> resolvedEntityTypes)
  {
    Dictionary<string, EntitySet> dictionary1 = new Dictionary<string, EntitySet>(unresolved.EntitySets.Count);
    foreach (KeyValuePair<string, UnresolvedEntitySet> entitySet1 in (IEnumerable<KeyValuePair<string, UnresolvedEntitySet>>) unresolved.EntitySets)
    {
      EntitySet entitySet2 = EntitySet.Resolve(entitySet1.Value, schema, resolvedEntityTypes);
      dictionary1.Add(entitySet1.Key, entitySet2);
    }
    Dictionary<string, FunctionImport> dictionary2 = new Dictionary<string, FunctionImport>(unresolved.FunctionImports.Count);
    foreach (KeyValuePair<string, UnresolvedFunctionImport> functionImport1 in (IEnumerable<KeyValuePair<string, UnresolvedFunctionImport>>) unresolved.FunctionImports)
    {
      FunctionImport functionImport2 = FunctionImport.Resolve(functionImport1.Value, schema);
      dictionary2.Add(functionImport1.Key, functionImport2);
    }
    return new EntityContainer()
    {
      EntitySets = (IReadOnlyDictionary<string, EntitySet>) dictionary1,
      FunctionImports = (IReadOnlyDictionary<string, FunctionImport>) dictionary2,
      IsDefaultEntityContainer = unresolved.IsDefaultEntityContainer,
      SupportedFormats = unresolved.SupportedFormats,
      AdditionalAttributes = unresolved.AdditionalAttributes
    };
  }
}
