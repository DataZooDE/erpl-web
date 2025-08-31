// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceRequestsV2
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Specialized;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class ServiceRequestsV2(IODataConnection connection) : ServiceRequests(connection)
{
  public override async Task<Stream> GetServicesAsync(
    string searchPattern,
    CancellationToken ct,
    uint maxRows = 0)
  {
    ServiceRequestsV2 serviceRequestsV2 = this;
    UriBuilder uriBuilder = new UriBuilder(serviceRequestsV2.ODataConnection.BaseUrl.TrimEnd('/') + "/sap/opu/odata/iwfnd/catalogservice;v=2/ServiceCollection");
    NameValueCollection queryString = HttpUtility.ParseQueryString(uriBuilder.Query);
    if (!string.IsNullOrEmpty(searchPattern))
      queryString["$filter"] = $"substringof('{searchPattern.Trim()}',TechnicalServiceName)";
    if (maxRows > 0U)
      queryString["$top"] = maxRows.ToString();
    uriBuilder.Query = queryString.ToString();
    string url = uriBuilder.ToString();
    return await serviceRequestsV2.SendAndGetResponseStreamAsync(url, ResponseFormat.JSON, ct);
  }

  public override async Task<Stream> GetServiceEntitiesAsync(Service service, CancellationToken ct)
  {
    return await this.SendAndGetResponseStreamAsync(service.ServiceUrl, ResponseFormat.JSON, ct);
  }

  protected override string MakeGetDataUrl(Service service, string endpoint)
  {
    return Path.Combine(service.ServiceUrl, endpoint);
  }
}
