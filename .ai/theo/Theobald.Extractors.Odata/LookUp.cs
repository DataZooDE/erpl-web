// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.LookUp
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.Odata;

public static class LookUp
{
  public static async Task<IEnumerable<Service>> SearchServicesAsync(
    IODataConnection connection,
    string searchPattern,
    CancellationToken ct)
  {
    Task<IEnumerable<Service>> servicesV2 = LookUp.GetServicesAsync(connection, searchPattern, ODataVersion.V2, ct);
    Task<IEnumerable<Service>> servicesV4 = LookUp.GetServicesAsync(connection, searchPattern, ODataVersion.V4, ct);
    await Task.WhenAll((Task) servicesV2, (Task) servicesV4);
    IEnumerable<Service> services = servicesV2.Result.Concat<Service>(servicesV4.Result);
    servicesV2 = (Task<IEnumerable<Service>>) null;
    servicesV4 = (Task<IEnumerable<Service>>) null;
    return services;
  }

  private static async Task<IEnumerable<Service>> GetServicesAsync(
    IODataConnection connection,
    string searchPattern,
    ODataVersion version,
    CancellationToken ct)
  {
    IEnumerable<Service> servicesAsync1;
    using (Stream servicesAsync2 = await ServiceFactory.GetServiceRequest(connection, version).GetServicesAsync(searchPattern, ct))
    {
      ServiceJsonReader jsonReader = ServiceFactory.GetJsonReader(connection, version);
      if (jsonReader is ServiceJsonReaderV4 serviceJsonReaderV4)
        serviceJsonReaderV4.ServiceIdFilter = searchPattern;
      servicesAsync1 = jsonReader.ReadServices(servicesAsync2, ct);
    }
    return servicesAsync1;
  }

  public static async Task<IEnumerable<string>> SearchServiceEndpointAsync(
    IODataConnection connection,
    Service service,
    CancellationToken ct)
  {
    IEnumerable<string> strings;
    using (Stream serviceEntitiesAsync = await ServiceFactory.GetServiceRequest(connection, service.Version).GetServiceEntitiesAsync(service, ct))
      strings = ServiceFactory.GetJsonReader(connection, service.Version).ReadServiceEntities(serviceEntitiesAsync, ct);
    return strings;
  }

  public static async Task<IEnumerable<ServiceField>> SearchServiceFieldsAsync(
    IODataConnection connection,
    Service service,
    string endpoint,
    CancellationToken ct)
  {
    IEnumerable<ServiceField> serviceFields;
    using (Stream metadataAsync = await ServiceFactory.GetServiceRequest(connection, service.Version).GetMetadataAsync(service, ct))
      serviceFields = ServiceFactory.GetXmlReader(service.Version).ReadServiceMetadata(metadataAsync, endpoint, ct);
    return serviceFields;
  }
}
