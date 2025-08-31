// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.Settings
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public sealed class Settings : JsonSerializableObject, IDeepCopyable
{
  private const bool DefaultTrackChanges = false;
  private const int DefaultPackageSize = 15000;
  private bool? trackChanges;
  private int? packageSize;

  public bool TrackChanges
  {
    get => this.trackChanges.GetValueOrDefault();
    set => this.trackChanges = !value ? new bool?() : new bool?(value);
  }

  public int PackageSize
  {
    get => this.packageSize ?? 15000;
    set => this.packageSize = value == 15000 ? new int?() : new int?(value);
  }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    switch (member)
    {
      case "trackChanges":
        this.TrackChanges = reader.ReadBoolean();
        break;
      case "packageSize":
        this.PackageSize = reader.ReadInt32();
        break;
      default:
        base.ReadJsonProperty(reader, member);
        break;
    }
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    writer.Write("trackChanges", this.trackChanges);
    writer.Write("packageSize", this.packageSize);
  }

  public object ObjectCopyDeep()
  {
    return (object) new Settings()
    {
      TrackChanges = this.TrackChanges,
      PackageSize = this.PackageSize
    };
  }
}
