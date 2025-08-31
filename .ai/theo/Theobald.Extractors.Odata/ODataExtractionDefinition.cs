// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ODataExtractionDefinition
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Theobald.Extractors.Common;
using Theobald.Extractors.Conversion;
using Theobald.Json;
using Theobald.Logging;

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class ODataExtractionDefinition : 
  JsonSerializableObject,
  IODataExtractionDefinition,
  IExtractionDefinition,
  IJsonSerializable,
  IDeepCopyable,
  IUsageDataCollectable
{
  public string TechnicalName => this.Service?.Name ?? string.Empty;

  public string Description => this.Service?.Description ?? string.Empty;

  public Service? Service { get; set; }

  public string? ServiceEntity { get; set; }

  internal IEnumerable<Selection> Selections
  {
    get
    {
      Service service = this.Service;
      return (service != null ? service.Fields.Where<ServiceField>((Func<ServiceField, bool>) (field => field.Selections.Count > 0)).SelectMany<ServiceField, Selection>((Func<ServiceField, IEnumerable<Selection>>) (field => (IEnumerable<Selection>) field.Selections)) : (IEnumerable<Selection>) null) ?? Enumerable.Empty<Selection>();
    }
  }

  public OdataParameterization ODataParameterization { get; private set; }

  public IComplexParameterizableObject Parameterization
  {
    get => (IComplexParameterizableObject) this.ODataParameterization;
  }

  public ODataExtractionSettings Settings { get; set; } = new ODataExtractionSettings();

  public CancellationToken? PreviewCancellationToken { get; set; }

  public ODataExtractionDefinition()
  {
    this.ODataParameterization = new OdataParameterization(this);
  }

  private ODataExtractionDefinition(ODataExtractionDefinition copyFrom)
  {
    this.Service = (Service) copyFrom.Service?.ObjectCopyDeep();
    this.ServiceEntity = copyFrom.ServiceEntity;
    this.ODataParameterization = copyFrom.ODataParameterization.WithDefinition(in copyFrom);
  }

  public IEnumerable<ResultColumn> NewResultColumns(ColumnNameConverter columnNameConverter)
  {
    foreach (ServiceField serviceField in (IEnumerable<ServiceField>) this.Service?.SelectedFields ?? Enumerable.Empty<ServiceField>())
      yield return serviceField.ToResultColumn(columnNameConverter, this.TechnicalName);
  }

  public IExtractor NewExtractor(IODataConnection connection)
  {
    return (IExtractor) new ODataExtractor(connection, this);
  }

  public IEnumerable<string> CollectSapObjectNames()
  {
    if (!string.IsNullOrWhiteSpace(this.TechnicalName))
      yield return this.TechnicalName;
  }

  public IResultTable RunPreview(IODataConnection connection, ILog logger)
  {
    if (this.Service == null)
      throw new InvalidOperationException("No OData service selected");
    if (this.ServiceEntity == null)
      throw new InvalidOperationException("No OData service entity selected");
    if (this.Service.SelectedFields.Count < 1)
      throw new InvalidOperationException("No OData service field selected");
    ServiceRequests serviceRequest = ServiceFactory.GetServiceRequest(connection, this.Service.Version);
    int num = this.Settings.RowsLimit == 0 ? 100 : Math.Min(100, this.Settings.RowsLimit);
    LogWithSource logWithSource = new LogWithSource((ILog) new NullLogger(), "ODataExtractor");
    CancellationToken ct1 = this.PreviewCancellationToken ?? CancellationToken.None;
    Service service = this.Service;
    string serviceEntity = this.ServiceEntity;
    CancellationToken ct2 = ct1;
    LogWithSource log = logWithSource;
    int rowsLimit = num;
    return (IResultTable) ODataResultTable.New(this, ServiceFactory.GetJsonReader(connection, this.Service.Version).ReadServiceData(serviceRequest.ReadServiceDataAsync(service, serviceEntity, ct2, log, 0, rowsLimit).Result ?? throw new InvalidOperationException("Expected Stream"), this.Service.SelectedFields, ct1));
  }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    if (reader == null)
      throw new ArgumentNullException(nameof (reader));
    switch (member)
    {
      case "Service":
        reader.ReadObject<Service>((Func<Service>) (() => this.Service = new Service()));
        this.ODataParameterization = this.ODataParameterization.WithDefinition(this);
        break;
      case "ServiceEntity":
        this.ServiceEntity = reader.ReadString();
        break;
      case "ComplexRuntimeParameters":
        ComplexRuntimeParameterSet runtimeParameters = reader.ReadObject<ComplexRuntimeParameterSet>();
        this.ODataParameterization.SetParameters(in runtimeParameters);
        break;
      case "Settings":
        this.Settings.ReadJson(reader);
        break;
      default:
        reader.Skip();
        break;
    }
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    if (writer == null)
      throw new ArgumentNullException(nameof (writer));
    base.WriteJsonFields(writer);
    writer.Write("Service", (IJsonSerializable) this.Service);
    writer.Write("ServiceEntity", this.ServiceEntity);
    if (this.ODataParameterization.CustomParameters.Count > 0)
      writer.Write("ComplexRuntimeParameters", (IJsonSerializable) this.ODataParameterization.CustomParameters);
    writer.Write("Settings", (IJsonSerializable) this.Settings);
  }

  public object ObjectCopyDeep() => (object) new ODataExtractionDefinition(this);
}
