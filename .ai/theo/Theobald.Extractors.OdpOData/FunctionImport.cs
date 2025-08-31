// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.FunctionImport
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

#nullable disable
namespace Theobald.Extractors.OdpOData;

public readonly struct FunctionImport
{
  public required ComplexType ReturnType { get; init; }

  public static FunctionImport Resolve(
    UnresolvedFunctionImport unresolved,
    UnresolvedDataServiceSchema schema)
  {
    NamedRef returnType = unresolved.ReturnType;
    if (returnType.Namespace != null && returnType.Namespace != schema.Namespace)
      throw new DifferentNamespaceNotSupportedException(returnType, schema.Namespace);
    ComplexType complexType;
    if (!schema.ComplexTypes.TryGetValue(returnType.Name, out complexType))
      throw new ComplexTypeNotFoundException(returnType);
    return new FunctionImport() { ReturnType = complexType };
  }
}
