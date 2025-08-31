// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ODataExtractionSettings
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.Odata;

public class ODataExtractionSettings : JsonSerializableObject, IDeepCopyable
{
  private const int DefaultPackageSize = 15000;
  private int? rowsLimit;
  private int packageSize = 15000;

  public int RowsLimit
  {
    get => this.rowsLimit.GetValueOrDefault();
    set
    {
      if (0 > value)
        throw new ArgumentOutOfRangeException(nameof (value), "must not be negative");
      this.rowsLimit = value == 0 ? new int?() : new int?(value);
    }
  }

  public int PackageSize
  {
    get => this.packageSize;
    set
    {
      this.packageSize = value >= 1 ? value : throw new ArgumentOutOfRangeException(nameof (value), "must be greater than zero");
    }
  }

  public ODataExtractionSettings()
  {
  }

  private ODataExtractionSettings(ODataExtractionSettings copyFrom)
  {
    this.RowsLimit = copyFrom.RowsLimit;
    this.PackageSize = copyFrom.PackageSize;
  }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    if (reader == null)
      throw new ArgumentNullException(nameof (reader));
    switch (member)
    {
      case "RowsLimit":
        this.RowsLimit = reader.ReadInt32();
        break;
      case "PackageSize":
        this.PackageSize = reader.ReadInt32();
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
    if (this.rowsLimit.HasValue)
      writer.Write("RowsLimit", new int?(this.RowsLimit));
    if (this.packageSize == 15000)
      return;
    writer.Write("PackageSize", new int?(this.PackageSize));
  }

  public object ObjectCopyDeep() => (object) new ODataExtractionSettings(this);
}
