// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.FilterQueryBuilder
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System.Collections.Generic;
using System.Text;

#nullable enable
namespace Theobald.Extractors.OdpOData;

internal static class FilterQueryBuilder
{
  public static string Build(IEnumerable<KeyValuePair<string, Field>> fields)
  {
    StringBuilder builder = new StringBuilder();
    foreach (KeyValuePair<string, Field> field in fields)
    {
      if (field.Value.Filter != null)
      {
        switch (field.Value.Filter)
        {
          case SingleValueFilter filter1:
            if (field.Value.FilterRestriction != FilterRestriction.SingleValue && field.Value.FilterRestriction != FilterRestriction.Interval && field.Value.FilterRestriction != FilterRestriction.Unrestricted)
              throw new SingleValueFilterNotAllowedException(field.Key);
            FilterQueryBuilder.AppendSingleValueFilter(builder, field.Key, filter1);
            continue;
          case MultiValueFilter filter2:
            if (field.Value.FilterRestriction != FilterRestriction.MultiValue && field.Value.FilterRestriction != FilterRestriction.Unrestricted)
              throw new MultiValueFilterNotAllowedException(field.Key);
            FilterQueryBuilder.AppendMultiValueFilter(builder, field.Key, filter2);
            continue;
          case IntervalFilter filter3:
            if (field.Value.FilterRestriction != FilterRestriction.Interval && field.Value.FilterRestriction != FilterRestriction.Unrestricted)
              throw new IntervalFilterNotAllowedException(field.Key);
            FilterQueryBuilder.AppendIntervalFilter(builder, field.Key, filter3);
            continue;
          default:
            continue;
        }
      }
    }
    return builder.ToString();
  }

  private static void AppendSingleValueFilter(
    StringBuilder builder,
    string fieldName,
    SingleValueFilter filter)
  {
    if (builder.Length > 0)
      builder.Append(" and ");
    string str = filter.Value?.AsFilterString() ?? "null";
    StringBuilder stringBuilder1 = builder;
    StringBuilder stringBuilder2 = stringBuilder1;
    StringBuilder.AppendInterpolatedStringHandler interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(4, 2, stringBuilder1);
    interpolatedStringHandler.AppendFormatted(fieldName);
    interpolatedStringHandler.AppendLiteral(" eq ");
    interpolatedStringHandler.AppendFormatted(str);
    ref StringBuilder.AppendInterpolatedStringHandler local = ref interpolatedStringHandler;
    stringBuilder2.Append(ref local);
  }

  private static void AppendMultiValueFilter(
    StringBuilder builder,
    string fieldName,
    MultiValueFilter filter)
  {
    if (filter.Values.Count == 0)
      return;
    builder.Append('(');
    bool flag = true;
    foreach (FilterValue filterValue in (IEnumerable<FilterValue>) filter.Values)
    {
      if (!flag)
        builder.Append(" or ");
      flag = false;
      StringBuilder stringBuilder1 = builder;
      StringBuilder stringBuilder2 = stringBuilder1;
      StringBuilder.AppendInterpolatedStringHandler interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(4, 2, stringBuilder1);
      interpolatedStringHandler.AppendFormatted(fieldName);
      interpolatedStringHandler.AppendLiteral(" eq ");
      interpolatedStringHandler.AppendFormatted(filterValue.AsFilterString());
      ref StringBuilder.AppendInterpolatedStringHandler local = ref interpolatedStringHandler;
      stringBuilder2.Append(ref local);
    }
    builder.Append(')');
  }

  private static void AppendIntervalFilter(
    StringBuilder builder,
    string fieldName,
    IntervalFilter filter)
  {
    if (filter.FromValue == null)
      return;
    if (builder.Length > 0)
      builder.Append(" and ");
    StringBuilder stringBuilder1 = builder;
    StringBuilder stringBuilder2 = stringBuilder1;
    StringBuilder.AppendInterpolatedStringHandler interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(4, 2, stringBuilder1);
    interpolatedStringHandler.AppendFormatted(fieldName);
    interpolatedStringHandler.AppendLiteral(" ge ");
    interpolatedStringHandler.AppendFormatted(filter.FromValue.AsFilterString());
    ref StringBuilder.AppendInterpolatedStringHandler local1 = ref interpolatedStringHandler;
    stringBuilder2.Append(ref local1);
    if (filter.ToValue == null)
      return;
    StringBuilder stringBuilder3 = builder;
    StringBuilder stringBuilder4 = stringBuilder3;
    interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(9, 2, stringBuilder3);
    interpolatedStringHandler.AppendLiteral(" and ");
    interpolatedStringHandler.AppendFormatted(fieldName);
    interpolatedStringHandler.AppendLiteral(" le ");
    interpolatedStringHandler.AppendFormatted(filter.ToValue.AsFilterString());
    ref StringBuilder.AppendInterpolatedStringHandler local2 = ref interpolatedStringHandler;
    stringBuilder4.Append(ref local2);
  }
}
