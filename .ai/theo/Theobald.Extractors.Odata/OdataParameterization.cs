// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.OdataParameterization
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using Theobald.Common;
using Theobald.Extractors.Common;
using Theobald.Logging;

#nullable enable
namespace Theobald.Extractors.Odata;

public readonly struct OdataParameterization : IComplexParameterizableObject
{
  private readonly ScalarRuntimeParameter rows;
  private readonly ParameterizedSelections parameterizedSelections;
  private readonly ODataExtractionDefinition definition;

  public bool SupportsScalarParameters => true;

  public bool SupportsBinaryParameters => false;

  public bool SupportsRecordParameters => false;

  public bool SupportsListParameters => false;

  public ComplexRuntimeParameterSet StaticExtractionParameters { get; }

  public ComplexRuntimeParameterSet DynamicExtractionParameters { get; }

  public ComplexRuntimeParameterSet CustomParameters => this.parameterizedSelections.Parameters;

  public OdataParameterization(
    ODataExtractionDefinition definition,
    in ParameterizedSelections parameterizedSelections)
  {
    this.DynamicExtractionParameters = new ComplexRuntimeParameterSet();
    this.definition = definition;
    this.parameterizedSelections = parameterizedSelections;
    this.rows = (ScalarRuntimeParameter) ScalarRuntimeParameter.NewNumber(nameof (rows), "Total rows limit", (Func<int>) (() => definition.Settings.RowsLimit));
    this.StaticExtractionParameters = new ComplexRuntimeParameterSet()
    {
      (RuntimeParameter) this.rows
    };
  }

  public OdataParameterization(in ODataExtractionDefinition definition)
    : this(definition, new ParameterizedSelections(definition.Selections))
  {
  }

  public void SetParameters(in ComplexRuntimeParameterSet runtimeParameters)
  {
    this.parameterizedSelections.SetParameters(in runtimeParameters);
  }

  public void ApplyCustomParameterValues(
    in Func<RuntimeParameter, object?> provideValue,
    in ILog logger)
  {
    this.parameterizedSelections.ApplyParameterValues(in provideValue, in logger);
  }

  public void ApplyParameterValues(in Func<RuntimeParameter, object?> provideValue, in ILog logger)
  {
    if (provideValue((RuntimeParameter) this.rows) is Number number)
      this.definition.Settings.RowsLimit = (int) number;
    this.ApplyCustomParameterValues(in provideValue, in logger);
  }

  private OdataParameterization(
    in ODataExtractionDefinition definition,
    in OdataParameterization template)
    : this(definition, template.parameterizedSelections.WithSelections(definition.Selections))
  {
  }

  public OdataParameterization WithDefinition(in ODataExtractionDefinition newDefinition)
  {
    return new OdataParameterization(in newDefinition, in this);
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
