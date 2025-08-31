// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceXmlReaderV4
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class ServiceXmlReaderV4 : ServiceXmlReader
{
  protected override string MetadataNamespace { get; } = "{http://docs.oasis-open.org/odata/ns/edm}";
}
