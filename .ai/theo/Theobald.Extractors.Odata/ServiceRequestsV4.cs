// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceRequestsV4
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

public sealed class ServiceRequestsV4(IODataConnection connection) : ServiceRequests(connection)
{
  public override async Task<Stream> GetServicesAsync(
    string filter,
    CancellationToken ct,
    uint maxRows = 0)
  {
    ServiceRequestsV4 serviceRequestsV4 = this;
    UriBuilder uriBuilder = new UriBuilder(serviceRequestsV4.ODataConnection.BaseUrl.TrimEnd('/') + "/sap/opu/odata4/iwfnd/config/default/iwfnd/catalog/0002/ServiceGroups?$expand=DefaultSystem($expand=Services()))");
    NameValueCollection queryString = HttpUtility.ParseQueryString(uriBuilder.Query);
    if (maxRows > 0U)
      queryString["$top"] = maxRows.ToString();
    uriBuilder.Query = queryString.ToString();
    string url = uriBuilder.ToString();
    return await serviceRequestsV4.SendAndGetResponseStreamAsync(url, ResponseFormat.JSON, ct);
  }

  public override async Task<Stream> GetServiceEntitiesAsync(Service service, CancellationToken ct)
  {
    return await this.SendAndGetResponseStreamAsync(service.ServiceUrl, ResponseFormat.JSON, ct);
  }

  protected override string MakeGetDataUrl(Service service, string endpoint)
  {
    int startIndex = service.ServiceUrl.IndexOf('?');
    return startIndex >= 0 ? service.ServiceUrl.Insert(startIndex, endpoint) : Path.Combine(service.ServiceUrl, endpoint);
  }
}
