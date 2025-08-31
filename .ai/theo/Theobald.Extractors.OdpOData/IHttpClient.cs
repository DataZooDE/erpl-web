// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.IHttpClient
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public interface IHttpClient
{
  Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token);
}
