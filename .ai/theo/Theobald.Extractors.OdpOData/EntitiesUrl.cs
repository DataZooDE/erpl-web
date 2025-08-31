// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.EntitiesUrl
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Text;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct EntitiesUrl : IODataUrl
{
  public required string RelativeServiceUrl { get; init; }

  public required string EntitySet { get; init; }

  public IReadOnlyList<string> Selections { get; init; }

  public string Filter { get; init; }

  public int? Top { get; init; }

  public string? DeltaToken { get; init; }

  public EntitiesUrl()
  {
    // ISSUE: reference to a compiler-generated field
    this.\u003CRelativeServiceUrl\u003Ek__BackingField = (string) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CEntitySet\u003Ek__BackingField = (string) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CTop\u003Ek__BackingField = new int?();
    // ISSUE: reference to a compiler-generated field
    this.\u003CDeltaToken\u003Ek__BackingField = (string) null;
    // ISSUE: reference to a compiler-generated field
    this.\u003CSelections\u003Ek__BackingField = (IReadOnlyList<string>) Array.Empty<string>();
    // ISSUE: reference to a compiler-generated field
    this.\u003CFilter\u003Ek__BackingField = string.Empty;
  }

  private string SelectionsToUrlString()
  {
    StringBuilder stringBuilder1 = new StringBuilder();
    foreach (string selection in (IEnumerable<string>) this.Selections)
    {
      StringBuilder stringBuilder2 = stringBuilder1;
      StringBuilder stringBuilder3 = stringBuilder2;
      StringBuilder.AppendInterpolatedStringHandler interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(1, 1, stringBuilder2);
      interpolatedStringHandler.AppendFormatted(selection);
      interpolatedStringHandler.AppendLiteral(",");
      ref StringBuilder.AppendInterpolatedStringHandler local = ref interpolatedStringHandler;
      stringBuilder3.Append(ref local);
    }
    if (stringBuilder1.Length > 0)
    {
      stringBuilder1.Insert(0, "$select=");
      stringBuilder1.Remove(checked (stringBuilder1.Length - 1), 1);
    }
    return stringBuilder1.ToString();
  }

  private string FiltersToUrlString()
  {
    return !string.IsNullOrEmpty(this.Filter) ? "$filter=" + this.Filter : string.Empty;
  }

  private string TopToUrlString()
  {
    if (!this.Top.HasValue)
      return string.Empty;
    return $"$top={this.Top.Value}";
  }

  private string DeltaTokenToString()
  {
    return !string.IsNullOrEmpty(this.DeltaToken) ? "!deltatoken=" + this.DeltaToken : string.Empty;
  }

  public override string ToString() => this.AsUrl(string.Empty);

  public string AsUrl(string baseUrl)
  {
    baseUrl += this.RelativeServiceUrl.Trim('/');
    StringBuilder urlBuilder = new StringBuilder(baseUrl);
    if (urlBuilder.Length > 0)
      urlBuilder.Append('/');
    StringBuilder stringBuilder1 = urlBuilder;
    StringBuilder stringBuilder2 = stringBuilder1;
    StringBuilder.AppendInterpolatedStringHandler interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(1, 1, stringBuilder1);
    interpolatedStringHandler.AppendFormatted(this.EntitySet);
    interpolatedStringHandler.AppendLiteral("/");
    ref StringBuilder.AppendInterpolatedStringHandler local = ref interpolatedStringHandler;
    stringBuilder2.Append(ref local);
    urlBuilder.Append("?$format=json");
    AppendQueryParameter(this.SelectionsToUrlString());
    AppendQueryParameter(this.FiltersToUrlString());
    AppendQueryParameter(this.TopToUrlString());
    AppendQueryParameter(this.DeltaTokenToString());
    return urlBuilder.ToString();

    void AppendQueryParameter(string param)
    {
      if (string.IsNullOrEmpty(param))
        return;
      StringBuilder urlBuilder = urlBuilder;
      StringBuilder stringBuilder = urlBuilder;
      StringBuilder.AppendInterpolatedStringHandler interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(1, 1, urlBuilder);
      interpolatedStringHandler.AppendLiteral("&");
      interpolatedStringHandler.AppendFormatted(param);
      ref StringBuilder.AppendInterpolatedStringHandler local = ref interpolatedStringHandler;
      stringBuilder.Append(ref local);
    }
  }
}
