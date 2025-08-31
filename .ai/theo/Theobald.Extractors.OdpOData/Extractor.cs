// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.Extractor
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Theobald.Extractors.Common;
using Theobald.Logging;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class Extractor(IOdpODataExtractionApi api, Definition definition) : IExtractor
{
  private const int PreviewRowCount = 100;
  private const string LogSource = "OdpODataExtractor";

  private (EntitiesUrl, List<KeyValuePair<string, Field>>) Prepare(ILog logger, int? top = null)
  {
    bool flag = definition.Fields.All<KeyValuePair<string, Field>>((Func<KeyValuePair<string, Field>, bool>) (f => f.Value.IsSelected));
    List<KeyValuePair<string, Field>> list = definition.Fields.Where<KeyValuePair<string, Field>>((Func<KeyValuePair<string, Field>, bool>) (f => f.Value.IsSelected)).ToList<KeyValuePair<string, Field>>();
    if (list.Count == 0)
      throw new NoFieldsSelectedException();
    List<string> stringList = new List<string>();
    if (!flag)
      stringList = list.Select<KeyValuePair<string, Field>, string>((Func<KeyValuePair<string, Field>, string>) (f => f.Key)).ToList<string>();
    string str = FilterQueryBuilder.Build((IEnumerable<KeyValuePair<string, Field>>) definition.Fields);
    logger.Log("OdpODataExtractor", LogLevel.Info, "Using filter: " + str);
    return (new EntitiesUrl()
    {
      RelativeServiceUrl = definition.RelativeServiceUrl,
      EntitySet = definition.EntitySetName,
      Selections = (IReadOnlyList<string>) stringList,
      Filter = str,
      Top = top
    }, list);
  }

  private async Task<DataSet> GetFirstPageAsync(ILog logger, CancellationToken cancellationToken)
  {
    (EntitiesUrl url, List<KeyValuePair<string, Field>> keyValuePairList) = this.Prepare(logger);
    HttpResponseMessage httpResponseMessage;
    if (definition.Settings.TrackChanges)
      httpResponseMessage = await this.RetrieveResultWithChangeTracking(url, logger, cancellationToken);
    else
      httpResponseMessage = await this.RetrieveFullAsync(url, logger, cancellationToken);
    HttpResponseMessage response = httpResponseMessage;
    if (!response.IsSuccessStatusCode)
      throw await Error.FromErrorResponseAsync(response);
    DataSet firstPageAsync = new DataSet()
    {
      Content = response.Content,
      Fields = (IReadOnlyList<KeyValuePair<string, Field>>) keyValuePairList
    };
    keyValuePairList = (List<KeyValuePair<string, Field>>) null;
    return firstPageAsync;
  }

  private async Task<DataSet> GetNextPage(
    string nextUrl,
    IReadOnlyList<KeyValuePair<string, Field>> fields,
    CancellationToken cancellationToken)
  {
    HttpResponseMessage pagedAsync = await api.GetPagedAsync((IODataUrl) new RawODataUrl(nextUrl), definition.Settings.PackageSize, cancellationToken);
    if (!pagedAsync.IsSuccessStatusCode)
      throw await Error.FromErrorResponseAsync(pagedAsync);
    return new DataSet()
    {
      Content = pagedAsync.Content,
      Fields = fields
    };
  }

  public async Task<DataSet> PreviewAsync(ILog logger, CancellationToken cancellationToken)
  {
    (EntitiesUrl url, List<KeyValuePair<string, Field>> keyValuePairList) = this.Prepare(logger, new int?(100));
    HttpResponseMessage pagedAsync = await api.GetPagedAsync((IODataUrl) url, 100, cancellationToken);
    if (!pagedAsync.IsSuccessStatusCode)
      throw await Error.FromErrorResponseAsync(pagedAsync);
    DataSet dataSet = new DataSet()
    {
      Content = pagedAsync.Content,
      Fields = (IReadOnlyList<KeyValuePair<string, Field>>) keyValuePairList
    };
    keyValuePairList = (List<KeyValuePair<string, Field>>) null;
    return dataSet;
  }

  private async Task<HttpResponseMessage> RetrieveFullAsync(
    EntitiesUrl url,
    ILog logger,
    CancellationToken cancellationToken)
  {
    logger.Log("OdpODataExtractor", LogLevel.Info, $"Performing full load with URL: {url}");
    return await api.GetPagedAsync((IODataUrl) url, definition.Settings.PackageSize, cancellationToken);
  }

  private async Task<HttpResponseMessage> RetrieveResultWithChangeTracking(
    EntitiesUrl url,
    ILog logger,
    CancellationToken cancellationToken)
  {
    logger.Log("OdpODataExtractor", LogLevel.Info, "Change tracking is active");
    if (!definition.SupportsChangeTracking)
      throw new ChangeTrackingNotSupportedException();
    string deltaToken = await api.DiscoverLatestTokenAsync(url, cancellationToken);
    if (deltaToken == null)
    {
      logger.Log("OdpODataExtractor", LogLevel.Info, "No existing delta token found");
      logger.Log("OdpODataExtractor", LogLevel.Info, $"Initial load with URL: {url}");
      return await api.GetWithChangeTrackingAsync(url, cancellationToken);
    }
    logger.Log("OdpODataExtractor", LogLevel.Info, "Using delta token " + deltaToken);
    logger.Log("OdpODataExtractor", LogLevel.Info, $"Loading changes with URL: {url}");
    return await api.GetWithDeltaTokenAsync(url, deltaToken, cancellationToken);
  }

  public void Run(Action<IResultTable> processResult, ILog logger)
  {
    this.Run(processResult, logger, CancellationToken.None);
  }

  public void Run(
    Action<IResultTable> processResult,
    ILog logger,
    CancellationToken cancellationToken)
  {
    try
    {
      cancellationToken.ThrowIfCancellationRequested();
      Task<DataSet> firstPageAsync = this.GetFirstPageAsync(logger, cancellationToken);
      firstPageAsync.Wait(cancellationToken);
      DataSet result1 = firstPageAsync.Result;
      Task<JsonResultV2> task1 = result1.ReadAsync();
      task1.Wait(cancellationToken);
      JsonResultV2 result2 = task1.Result;
      processResult((IResultTable) result2);
      while (true)
      {
        string nextUrl = result2.NextUrl;
        if (nextUrl != null)
        {
          cancellationToken.ThrowIfCancellationRequested();
          Task<DataSet> nextPage = this.GetNextPage(nextUrl, result1.Fields, cancellationToken);
          nextPage.Wait(cancellationToken);
          result1 = nextPage.Result;
          Task<JsonResultV2> task2 = result1.ReadAsync();
          task2.Wait(cancellationToken);
          result2 = task2.Result;
          processResult((IResultTable) result2);
        }
        else
          break;
      }
    }
    catch (AggregateException ex)
    {
      if (ex.InnerExceptions.Count == 1)
        ExceptionDispatchInfo.Capture(ex.InnerExceptions[0]).Throw();
      throw;
    }
  }

  public void Finish(bool success, ILog logger)
  {
  }
}
