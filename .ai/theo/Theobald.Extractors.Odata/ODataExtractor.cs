// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ODataExtractor
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Theobald.Extractors.Common;
using Theobald.Logging;

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class ODataExtractor(
  IODataConnection connection,
  ODataExtractionDefinition definition) : ODataExtractorBase<ODataExtractionDefinition>(connection, definition)
{
  protected override string LogSource => nameof (ODataExtractor);

  protected override void Run(
    Action<IResultTable> processResult,
    CancellationToken cancellationToken)
  {
    cancellationToken.ThrowIfCancellationRequested();
    if (this.Definition.Service == null)
      throw new InvalidOperationException("No OData service selected");
    if (this.Definition.ServiceEntity == null)
      throw new InvalidOperationException("No OData service entity selected");
    if (this.Definition.Service.SelectedFields.Count < 1)
      throw new InvalidOperationException("No OData service field selected");
    this.Log(LogLevel.Info, "Starting OData extractor");
    this.Log(LogLevel.Info, $"Odata service {this.Definition.Service.Name} {this.Definition.Service.Version} entity {this.Definition.ServiceEntity}");
    ServiceRequests serviceRequest = ServiceFactory.GetServiceRequest(this.Connection, this.Definition.Service.Version);
    this.Log(LogLevel.Info, "Sending request to get data from Odata service");
    int num = 1;
    int offset = 0;
label_7:
    cancellationToken.ThrowIfCancellationRequested();
    this.Log(LogLevel.Info, $"Requesting package #{num}");
    int rowsLimit = this.Definition.Settings.RowsLimit == 0 || checked (offset + this.Definition.Settings.PackageSize) < this.Definition.Settings.RowsLimit ? this.Definition.Settings.PackageSize : checked (this.Definition.Settings.RowsLimit - offset);
    using (Task<Stream> task = serviceRequest.ReadServiceDataAsync(this.Definition.Service, this.Definition.ServiceEntity, cancellationToken, this.RunLogger.GetValueOrDefault(), offset, rowsLimit))
    {
      this.Log(LogLevel.Info, $"Reading data from package #{num}");
      Stream result = task.Result;
      if (result != null)
      {
        IList<object[]> results = ServiceFactory.GetJsonReader(this.Connection, this.Definition.Service.Version).ReadServiceData(result, this.Definition.Service.SelectedFields, cancellationToken);
        processResult((IResultTable) ODataResultTable.New(this.Definition, results));
        this.Log(LogLevel.Info, $"Processed package #{num} with {results.Count} rows");
        checked { offset += results.Count; }
        checked { ++num; }
        if (this.Definition.Settings.RowsLimit > 0 && offset >= this.Definition.Settings.RowsLimit)
          return;
        if (results.Count >= this.Definition.Settings.PackageSize)
          goto label_7;
      }
      else
        goto label_7;
    }
  }
}
