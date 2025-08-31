// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.ODataClient
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public class ODataClient(IHttpClient httpClient, IODataConnection connection) : IODataClient
{
  private static readonly Version ODataVersion = new Version(2, 0);

  public IODataConnection Connection { get; } = connection;

  private void AddCredentialsToRequest(HttpRequestMessage request)
  {
    if (string.IsNullOrEmpty(this.Connection.Username) || string.IsNullOrEmpty(this.Connection.Password))
      return;
    string base64String = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{this.Connection.Username}:{this.Connection.Password}"));
    request.Headers.Add("Authorization", "Basic " + base64String);
  }

  private static void AddODataVersionToRequest(HttpRequestMessage request)
  {
    string str = $"{ODataClient.ODataVersion.Major}.{ODataClient.ODataVersion.Minor}";
    request.Headers.Add("DataServiceVersion", str);
    request.Headers.Add("MaxDataServiceVersion", str);
  }

  public async Task<HttpResponseMessage> GetMetadataAsync(
    string serviceUrl,
    CancellationToken cancellationToken)
  {
    HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, serviceUrl.TrimEnd('/') + "/$metadata");
    this.AddCredentialsToRequest(request);
    ODataClient.AddODataVersionToRequest(request);
    request.Headers.Accept.Clear();
    request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/xml"));
    return await httpClient.SendAsync(request, cancellationToken);
  }

  private static void EnsureResponseIsForCorrectODataVersion(HttpResponseMessage response)
  {
    IEnumerable<string> values;
    if (!response.Headers.TryGetValues("DataServiceVersion", out values))
      throw new VersionInformationMissingInResponseException();
    foreach (string str in values)
    {
      if (Version.Parse(str) > ODataClient.ODataVersion)
        throw new InvalidServiceVersionInResponseException(str);
    }
  }

  public async Task<HttpResponseMessage> GetAsync(
    IODataUrl url,
    IEnumerable<KeyValuePair<string, string>> additionalHeaders,
    CancellationToken cancellationToken)
  {
    string baseUrl = this.Connection.BaseUrl;
    if (!baseUrl.EndsWith("/"))
      baseUrl += "/";
    HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, url.AsUrl(baseUrl));
    this.AddCredentialsToRequest(request);
    ODataClient.AddODataVersionToRequest(request);
    request.Headers.Accept.Clear();
    request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));
    foreach (KeyValuePair<string, string> additionalHeader in additionalHeaders)
      request.Headers.Add(additionalHeader.Key, additionalHeader.Value);
    HttpResponseMessage response = await httpClient.SendAsync(request, cancellationToken);
    ODataClient.EnsureResponseIsForCorrectODataVersion(response);
    return response;
  }
}
