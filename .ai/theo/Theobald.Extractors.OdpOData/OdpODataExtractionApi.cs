// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.OdpODataExtractionApi
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public class OdpODataExtractionApi(IODataClient client) : IOdpODataExtractionApi
{
  public async Task<HttpResponseMessage> GetWithChangeTrackingAsync(
    EntitiesUrl url,
    CancellationToken cancellationToken)
  {
    // ISSUE: object of a compiler-generated type is created
    HttpResponseMessage async = await client.GetAsync((IODataUrl) url, (IEnumerable<KeyValuePair<string, string>>) new \u003C\u003Ez__ReadOnlySingleElementList<KeyValuePair<string, string>>(new KeyValuePair<string, string>("Prefer", "odata.track-changes")), cancellationToken);
    if (async.IsSuccessStatusCode && !async.Headers.GetValues("preference-applied").Any<string>((Func<string, bool>) (v => v == "odata.track-changes")))
      throw new DeltaPreferenceNotAppliedException();
    return async;
  }

  public async Task<HttpResponseMessage> GetPagedAsync(
    IODataUrl url,
    int pageSize,
    CancellationToken cancellationToken)
  {
    // ISSUE: object of a compiler-generated type is created
    return await client.GetAsync(url, (IEnumerable<KeyValuePair<string, string>>) new \u003C\u003Ez__ReadOnlySingleElementList<KeyValuePair<string, string>>(new KeyValuePair<string, string>("Prefer", $"odata.maxpagesize={pageSize}")), cancellationToken);
  }

  public Task<HttpResponseMessage> GetFullAsync(IODataUrl url, CancellationToken cancellationToken)
  {
    return client.GetAsync(url, (IEnumerable<KeyValuePair<string, string>>) Array.Empty<KeyValuePair<string, string>>(), cancellationToken);
  }

  public async Task<HttpResponseMessage> GetWithDeltaTokenAsync(
    EntitiesUrl url,
    string deltaToken,
    CancellationToken cancellationToken)
  {
    return await client.GetAsync((IODataUrl) new EntitiesUrl()
    {
      RelativeServiceUrl = url.RelativeServiceUrl,
      EntitySet = url.EntitySet,
      Filter = url.Filter,
      Selections = url.Selections,
      DeltaToken = deltaToken
    }, (IEnumerable<KeyValuePair<string, string>>) Array.Empty<KeyValuePair<string, string>>(), cancellationToken);
  }

  public async Task<string?> DiscoverLatestTokenAsync(
    EntitiesUrl url,
    CancellationToken cancellationToken)
  {
    // ISSUE: object of a compiler-generated type is created
    HttpResponseMessage async = await client.GetAsync((IODataUrl) new EntitiesUrl()
    {
      RelativeServiceUrl = url.RelativeServiceUrl,
      EntitySet = ("DeltaLinksOf" + url.EntitySet),
      Selections = (IReadOnlyList<string>) new \u003C\u003Ez__ReadOnlyArray<string>(new string[2]
      {
        "DeltaToken",
        "CreatedAt"
      })
    }, (IEnumerable<KeyValuePair<string, string>>) Array.Empty<KeyValuePair<string, string>>(), cancellationToken);
    if (async.StatusCode == HttpStatusCode.NotFound)
      throw new DeltaLinksEntitySetDoesNotExistException(url.EntitySet);
    async.EnsureSuccessStatusCode();
    // ISSUE: object of a compiler-generated type is created
    JsonReaderV2 jsonReaderV2 = new JsonReaderV2((IReadOnlyList<KeyValuePair<string, Field>>) new \u003C\u003Ez__ReadOnlyArray<KeyValuePair<string, Field>>(new KeyValuePair<string, Field>[2]
    {
      new KeyValuePair<string, Field>("DeltaToken", new Field()
      {
        Type = EdmType.String,
        IsNullable = false
      }),
      new KeyValuePair<string, Field>("CreatedAt", new Field()
      {
        Type = EdmType.DateTime,
        IsNullable = false
      })
    }));
    JsonResultV2 jsonResultV2 = jsonReaderV2.Read(await async.Content.ReadAsStreamAsync());
    jsonReaderV2 = (JsonReaderV2) null;
    string str = (string) null;
    DateTime minValue = DateTime.MinValue;
    foreach (object[] result in jsonResultV2.Results)
    {
      DateTime? nullable = (DateTime?) result[1];
      if (!nullable.HasValue)
        throw new InvalidDataException("Creation date for delta is missing");
      if (!(nullable.Value <= minValue))
      {
        minValue = nullable.Value;
        str = (string) result[0];
        if (str == null)
          throw new InvalidDataException("Delta token missing for delta");
      }
    }
    return str;
  }

  public async Task TerminateSubscriptionAsync(EntitiesUrl url, CancellationToken cancellationToken)
  {
    EntitiesUrl functionUrl = new EntitiesUrl()
    {
      RelativeServiceUrl = url.RelativeServiceUrl,
      EntitySet = "TerminateDeltasFor" + url.EntitySet
    };
    HttpResponseMessage async = await client.GetAsync((IODataUrl) functionUrl, (IEnumerable<KeyValuePair<string, string>>) Array.Empty<KeyValuePair<string, string>>(), cancellationToken);
    if (async.StatusCode == HttpStatusCode.NotFound)
      throw new TerminateDeltasFunctionDoesNotExistException(url.EntitySet);
    using (Stream stream = await async.Content.ReadAsStreamAsync())
    {
      if (!TerminateSubscriptionResult.ReadFromJson(stream, functionUrl.EntitySet).Success)
        throw new SubscriptionTerminationFailedException();
    }
    functionUrl = new EntitiesUrl();
  }
}
