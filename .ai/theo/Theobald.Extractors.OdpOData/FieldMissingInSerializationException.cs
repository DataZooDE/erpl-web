// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.FieldMissingInSerializationException
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class FieldMissingInSerializationException(string name) : Exception($"Field `{name}` is missing in serialization or is set to null")
{
  public string FieldName { get; } = name;
}
