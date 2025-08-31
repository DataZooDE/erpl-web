// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.InvalidErrorException
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Net;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public class InvalidErrorException(HttpStatusCode httpStatusCode, Exception inner) : Exception($"Server indicated an error ({httpStatusCode}) but did not provide a valid error response", inner)
{
  public HttpStatusCode HttpStatusCode { get; } = httpStatusCode;
}
