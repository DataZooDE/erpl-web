// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceFactory
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.Odata;

public static class ServiceFactory
{
  public static ServiceRequests GetServiceRequest(IODataConnection connection, ODataVersion version)
  {
    return version != ODataVersion.V4 ? (ServiceRequests) new ServiceRequestsV2(connection) : (ServiceRequests) new ServiceRequestsV4(connection);
  }

  public static ServiceJsonReader GetJsonReader(IODataConnection connection, ODataVersion version)
  {
    return version != ODataVersion.V4 ? (ServiceJsonReader) new ServiceJsonReaderV2() : (ServiceJsonReader) new ServiceJsonReaderV4(connection.BaseUrl);
  }

  public static ServiceXmlReader GetXmlReader(ODataVersion version)
  {
    return version != ODataVersion.V4 ? (ServiceXmlReader) new ServiceXmlReaderV2() : (ServiceXmlReader) new ServiceXmlReaderV4();
  }
}
