// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceRequests
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Theobald.Extractors.Common;
using Theobald.Logging;

#nullable enable
namespace Theobald.Extractors.Odata;

public abstract class ServiceRequests
{
  private static readonly HttpClient HttpClientInstance = new HttpClient();
  private static byte RetryMaxTrials = 3;
  private static byte DelayBeforeRetrySeconds = 3;

  private static IEnumerable<HttpStatusCode> RetriableStatuses { get; } = (IEnumerable<HttpStatusCode>) new HttpStatusCode[5]
  {
    HttpStatusCode.BadGateway,
    HttpStatusCode.Conflict,
    HttpStatusCode.Forbidden,
    HttpStatusCode.GatewayTimeout,
    HttpStatusCode.RequestTimeout
  };

  internal IODataConnection ODataConnection { get; }

  protected ServiceRequests(IODataConnection connection)
  {
    this.ODataConnection = connection != null ? connection : throw new ArgumentNullException(nameof (connection));
  }

  private void AddHeaders(HttpRequestMessage request, ResponseFormat format)
  {
    string base64String = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{this.ODataConnection.Username}:{this.ODataConnection.Password}"));
    request.Headers.Add("Authorization", "Basic " + base64String);
    if (format == ResponseFormat.XML)
      request.Headers.Add("Accept", "application/xml");
    else
      request.Headers.Add("Accept", "application/json");
  }

  private async Task<HttpResponseMessage> SendAsync(
    string url,
    ResponseFormat format,
    CancellationToken ct)
  {
    HttpResponseMessage httpResponseMessage1;
    using (HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, url))
    {
      this.AddHeaders(request, format);
      HttpResponseMessage httpResponseMessage2 = await ServiceRequests.HttpClientInstance.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
      ct.ThrowIfCancellationRequested();
      httpResponseMessage1 = httpResponseMessage2;
    }
    return httpResponseMessage1;
  }

  protected async Task<Stream> SendAndGetResponseStreamAsync(
    string url,
    ResponseFormat format,
    CancellationToken ct)
  {
    HttpResponseMessage response = await this.SendAsync(url, format, ct);
    if (!response.IsSuccessStatusCode)
    {
      string str = await response.Content.ReadAsStringAsync();
      throw new HttpRequestException($"Error {(int) response.StatusCode}-{response.StatusCode}\n{str}");
    }
    Stream responseStreamAsync = await response.Content.ReadAsStreamAsync();
    response = (HttpResponseMessage) null;
    return responseStreamAsync;
  }

  private async Task<Stream> SendAndGetResponseStreamWithRetryAsync(
    string url,
    ResponseFormat format,
    CancellationToken ct,
    LogWithSource log,
    byte retryNumber = 0)
  {
    HttpResponseMessage response = await this.SendAsync(url, format, ct);
    ct.ThrowIfCancellationRequested();
    if (!response.IsSuccessStatusCode)
    {
      if (ServiceRequests.RetriableStatuses.Contains<HttpStatusCode>(response.StatusCode) && (int) retryNumber < (int) ServiceRequests.RetryMaxTrials)
      {
        log.Log(LogLevel.Warning, FormattableStringFactory.Create("Request failed with status {0}-{1}. A new request will be sent in {2} seconds...", (object) (int) response.StatusCode, (object) response.StatusCode, (object) ServiceRequests.DelayBeforeRetrySeconds));
        await Task.Delay(TimeSpan.FromSeconds((double) ServiceRequests.DelayBeforeRetrySeconds), ct);
        Stream streamWithRetryAsync = await this.SendAndGetResponseStreamWithRetryAsync(url, format, ct, log, checked (++retryNumber));
      }
      else
      {
        string str = await response.Content.ReadAsStringAsync();
        throw new HttpRequestException($"Error {(int) response.StatusCode}-{response.StatusCode}\n{str}");
      }
    }
    Stream streamWithRetryAsync1 = await response.Content.ReadAsStreamAsync();
    response = (HttpResponseMessage) null;
    return streamWithRetryAsync1;
  }

  public abstract Task<Stream> GetServicesAsync(string filter, CancellationToken ct, uint maxRows = 0);

  public abstract Task<Stream> GetServiceEntitiesAsync(Service service, CancellationToken ct);

  public async Task<Stream> GetMetadataAsync(Service service, CancellationToken ct)
  {
    return await this.SendAndGetResponseStreamAsync(service.MetadataUrl, ResponseFormat.XML, ct);
  }

  protected abstract string MakeGetDataUrl(Service service, string endpoint);

  private string? GetFilters(Service service)
  {
    List<ServiceField> list = service.SelectedFields.Where<ServiceField>((Func<ServiceField, bool>) (field => field.Selections.Any<Selection>())).ToList<ServiceField>();
    if (list.Count < 1)
      return (string) null;
    StringBuilder stringBuilder = new StringBuilder();
    foreach (ServiceField serviceField in list)
    {
      stringBuilder.Append(serviceField.SelectionsToString());
      if (list.Last<ServiceField>() != serviceField)
        stringBuilder.Append(" and ");
    }
    return stringBuilder.ToString();
  }

  private string? GetOrderBy(Service service)
  {
    List<ServiceField> list = service.SelectedFields.Where<ServiceField>((Func<ServiceField, bool>) (field => field.IsPrimaryKey)).ToList<ServiceField>();
    StringBuilder stringBuilder = new StringBuilder();
    if (list.Count > 0)
    {
      foreach (ServiceField serviceField in list)
      {
        stringBuilder.Append(serviceField.Name);
        if (list.Last<ServiceField>() != serviceField)
          stringBuilder.Append(",");
      }
      stringBuilder.Append(" asc");
    }
    return stringBuilder.ToString();
  }

  public async Task<Stream> ReadServiceDataAsync(
    Service service,
    string endpoint,
    CancellationToken ct,
    LogWithSource log,
    int offset,
    int rowsLimit)
  {
    UriBuilder uriBuilder = new UriBuilder(this.MakeGetDataUrl(service, endpoint));
    NameValueCollection queryString = HttpUtility.ParseQueryString(uriBuilder.Query);
    string orderBy = this.GetOrderBy(service);
    if (!string.IsNullOrEmpty(orderBy))
      queryString["orderby"] = orderBy;
    queryString["$top"] = rowsLimit.ToString();
    queryString["$skip"] = offset.ToString();
    if (service.SelectedFields.Count != service.Fields.Count)
      queryString["$select"] = string.Join(",", service.SelectedFields.Select<ServiceField, string>((Func<ServiceField, string>) (field => field.Name)));
    string filters = this.GetFilters(service);
    if (!string.IsNullOrEmpty(filters))
      queryString["$filter"] = filters;
    uriBuilder.Query = queryString.ToString();
    string url = uriBuilder.ToString();
    log.Log(LogLevel.Info, FormattableStringFactory.Create("{0}", (object) url));
    ct.ThrowIfCancellationRequested();
    return await this.SendAndGetResponseStreamWithRetryAsync(url, ResponseFormat.JSON, ct, log);
  }

  public static async Task TestOdataConnectionAsync(
    string baseUrl,
    string userName,
    string password,
    ODataVersion version,
    CancellationToken ct)
  {
    using (await ServiceFactory.GetServiceRequest((IODataConnection) new OdataConnectionDefinition()
    {
      BaseUrl = baseUrl,
      Password = password,
      Username = userName
    }, version).GetServicesAsync("*", ct, 1U))
      ;
  }
}
