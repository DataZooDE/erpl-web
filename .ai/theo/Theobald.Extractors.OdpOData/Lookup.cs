// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.Lookup
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public static class Lookup
{
  public static bool IsOdpRelatedEntitySetName(string name)
  {
    return name.StartsWith("EntityOf", StringComparison.InvariantCultureIgnoreCase) || name.StartsWith("FactsOf", StringComparison.InvariantCultureIgnoreCase);
  }

  private static bool GuessIsOdpService(IReadOnlyList<string> entitySets)
  {
    // ISSUE: reference to a compiler-generated field
    // ISSUE: reference to a compiler-generated field
    return entitySets.Any<string>(Lookup.\u003C\u003EO.\u003C0\u003E__IsOdpRelatedEntitySetName ?? (Lookup.\u003C\u003EO.\u003C0\u003E__IsOdpRelatedEntitySetName = new Func<string, bool>(Lookup.IsOdpRelatedEntitySetName)));
  }

  public static async Task<List<ServiceCatalogEntry>> SearchAsync(
    IOdpODataLookupApi api,
    LookupUrl url,
    CancellationToken cancellationToken)
  {
    return (await api.SearchAsync(url, cancellationToken)).Where<ServiceCatalogEntry>((Func<ServiceCatalogEntry, bool>) (e => Lookup.GuessIsOdpService(e.EntitySets))).ToList<ServiceCatalogEntry>();
  }
}
