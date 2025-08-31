// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.OdpODataParameterization
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using System.Collections.Generic;
using System.Linq;
using Theobald.Extractors.Common;
using Theobald.Logging;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class OdpODataParameterization : IComplexParameterizableObject
{
  public required Definition Definition { get; init; }

  public bool SupportsScalarParameters => true;

  public bool SupportsBinaryParameters => false;

  public bool SupportsRecordParameters => false;

  public bool SupportsListParameters => false;

  public ComplexRuntimeParameterSet StaticExtractionParameters { get; } = new ComplexRuntimeParameterSet();

  public ComplexRuntimeParameterSet DynamicExtractionParameters { get; } = new ComplexRuntimeParameterSet();

  public ComplexRuntimeParameterSet CustomParameters { get; } = new ComplexRuntimeParameterSet();

  public void SetParameters(in ComplexRuntimeParameterSet runtimeParameters)
  {
    this.CustomParameters.Clear();
    foreach (RuntimeParameter runtimeParameter in runtimeParameters)
      this.CustomParameters.Add(runtimeParameter);
    this.CorrectRuntimeParameterReferences();
  }

  private void CorrectRuntimeParameterReferences()
  {
    foreach (KeyValuePair<string, Field> field in (IEnumerable<KeyValuePair<string, Field>>) this.Definition.Fields)
      this.CorrectParameterReferenceForFilter(field.Value.Filter);
  }

  private void CorrectParameterReferenceForFilter(IFilter? filter)
  {
    switch (filter)
    {
      case SingleValueFilter singleValueFilter:
        RuntimeParameter runtimeParameter1 = singleValueFilter.RuntimeParameter;
        if (runtimeParameter1 == null)
          break;
        singleValueFilter.RuntimeParameter = this.CustomParameters.GetParameterOrNull(runtimeParameter1.Name);
        break;
      case MultiValueFilter multiValueFilter:
        ListRuntimeParameter runtimeParameter2 = multiValueFilter.RuntimeParameter;
        if (runtimeParameter2 == null)
          break;
        multiValueFilter.RuntimeParameter = this.CustomParameters.GetParameterOrNull(runtimeParameter2.Name) as ListRuntimeParameter;
        break;
      case IntervalFilter interval:
        this.CorrectParameterReferenceForIntervalFilter(interval);
        break;
    }
  }

  private void CorrectParameterReferenceForIntervalFilter(IntervalFilter interval)
  {
    ScalarRuntimeParameter fromParameter = interval.FromParameter;
    string name;
    if (fromParameter != null)
    {
      IntervalFilter intervalFilter = interval;
      ComplexRuntimeParameterSet customParameters = this.CustomParameters;
      name = fromParameter.Name;
      ref string local = ref name;
      ScalarRuntimeParameter parameterOrNull = customParameters.GetParameterOrNull(in local) as ScalarRuntimeParameter;
      intervalFilter.FromParameter = parameterOrNull;
    }
    ScalarRuntimeParameter toParameter = interval.ToParameter;
    if (toParameter == null)
      return;
    IntervalFilter intervalFilter1 = interval;
    ComplexRuntimeParameterSet customParameters1 = this.CustomParameters;
    name = toParameter.Name;
    ref string local1 = ref name;
    ScalarRuntimeParameter parameterOrNull1 = customParameters1.GetParameterOrNull(in local1) as ScalarRuntimeParameter;
    intervalFilter1.ToParameter = parameterOrNull1;
  }

  public void ApplyCustomParameterValues(
    in Func<RuntimeParameter, object?> provideValue,
    in ILog logger)
  {
    foreach (KeyValuePair<string, Field> field in (IEnumerable<KeyValuePair<string, Field>>) this.Definition.Fields)
      OdpODataParameterization.ApplyParametersForFilter(provideValue, field.Value);
  }

  private static void ApplyParametersForFilter(
    Func<RuntimeParameter, object?> provideValue,
    Field field)
  {
    switch (field.Filter)
    {
      case SingleValueFilter singleValueFilter:
        if (singleValueFilter.RuntimeParameter == null)
          break;
        object obj = provideValue(singleValueFilter.RuntimeParameter);
        if (obj == null)
        {
          singleValueFilter.Value = (FilterValue) null;
          break;
        }
        singleValueFilter.Value = new FilterValue(field.Type);
        singleValueFilter.Value.SetValue(obj);
        break;
      case MultiValueFilter multiValueFilter:
        if (multiValueFilter.RuntimeParameter == null || !(provideValue((RuntimeParameter) multiValueFilter.RuntimeParameter) is IEnumerable<object> source))
          break;
        multiValueFilter.Values = (IList<FilterValue>) source.Select<object, FilterValue>((Func<object, FilterValue>) (v => new FilterValue(field.Type, v))).ToList<FilterValue>();
        break;
      case IntervalFilter intervalFilter:
        if (intervalFilter.FromParameter != null)
          intervalFilter.FromValue = new FilterValue(field.Type, provideValue((RuntimeParameter) intervalFilter.FromParameter));
        if (intervalFilter.ToParameter == null)
          break;
        intervalFilter.ToValue = new FilterValue(field.Type, provideValue((RuntimeParameter) intervalFilter.ToParameter));
        break;
    }
  }

  public void ApplyParameterValues(in Func<RuntimeParameter, object?> provideValue, in ILog logger)
  {
    this.ApplyCustomParameterValues(in provideValue, in logger);
  }

  void IComplexParameterizableObject.SetParameters(in ComplexRuntimeParameterSet runtimeParameters)
  {
    this.SetParameters(in runtimeParameters);
  }

  void IComplexParameterizableObject.ApplyCustomParameterValues(
    in Func<RuntimeParameter, object?> provideValue,
    in ILog logger)
  {
    this.ApplyCustomParameterValues(in provideValue, in logger);
  }

  void IComplexParameterizableObject.ApplyParameterValues(
    in Func<RuntimeParameter, object?> provideValue,
    in ILog logger)
  {
    this.ApplyParameterValues(in provideValue, in logger);
  }
}
