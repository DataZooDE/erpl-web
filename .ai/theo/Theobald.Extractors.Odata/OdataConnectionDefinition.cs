// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.OdataConnectionDefinition
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class OdataConnectionDefinition : 
  JsonSerializableObject,
  IODataConnection,
  IConnectionDefinition,
  IJsonSerializable
{
  public ConnectionType ConnectionType => ConnectionType.OData;

  public string BaseUrl { get; set; } = string.Empty;

  public string Username { get; set; } = string.Empty;

  public string Password { get; set; } = string.Empty;

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    switch (member)
    {
      case "BaseUrl":
        this.BaseUrl = reader.ReadString();
        break;
      case "Username":
        this.Username = reader.ReadString();
        break;
      case "Password":
        this.Password = reader.ReadString();
        break;
      default:
        reader.Skip();
        break;
    }
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    base.WriteJsonFields(writer);
    writer.Write("BaseUrl", this.BaseUrl);
    writer.Write("Username", this.Username);
    writer.Write("Password", this.Password);
  }
}
