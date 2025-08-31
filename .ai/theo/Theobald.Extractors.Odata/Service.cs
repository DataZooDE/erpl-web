// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.Service
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using System;
using System.Collections.Generic;
using System.Linq;
using Theobald.Extractors.Common;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.Odata;

public class Service : JsonSerializableObject, IDeepCopyable
{
  public IList<ServiceField> Fields = (IList<ServiceField>) new List<ServiceField>();

  public string Id { get; set; } = string.Empty;

  public string Name { get; set; } = string.Empty;

  public string Description { get; set; } = string.Empty;

  public string ServiceUrl { get; set; } = string.Empty;

  public ODataVersion Version { get; set; }

  public string MetadataUrl
  {
    get
    {
      return this.ServiceUrl.IndexOf("?") < 0 ? this.ServiceUrl + "$metadata" : this.ServiceUrl.Substring(0, this.ServiceUrl.IndexOf("?")) + "$metadata";
    }
  }

  public IList<ServiceField> SelectedFields
  {
    get
    {
      return (IList<ServiceField>) this.Fields.Where<ServiceField>((Func<ServiceField, bool>) (field => field.IsSelected)).ToList<ServiceField>();
    }
  }

  internal Service()
  {
  }

  public Service(
    string id,
    string name,
    string description,
    string serviceUrl,
    ODataVersion version)
  {
    if (id == null)
      throw new ArgumentNullException(nameof (id));
    if (name == null)
      throw new ArgumentNullException(nameof (name));
    if (description == null)
      throw new ArgumentNullException(nameof (description));
    if (serviceUrl == null)
      throw new ArgumentNullException(nameof (serviceUrl));
    this.Id = id;
    this.Name = name;
    this.Description = description;
    this.ServiceUrl = serviceUrl?.TrimEnd('/') + "/";
    this.Version = version;
  }

  private Service(Service copyFrom)
  {
    Service service = new Service();
    this.Id = copyFrom.Id;
    this.Name = copyFrom.Name;
    this.Description = copyFrom.Description;
    this.ServiceUrl = copyFrom.ServiceUrl;
    this.Version = copyFrom.Version;
    service.Fields = (IList<ServiceField>) new List<ServiceField>(this.Fields.Select<ServiceField, ServiceField>((Func<ServiceField, ServiceField>) (field => (ServiceField) field.ObjectCopyDeep())));
    foreach (ServiceField field in (IEnumerable<ServiceField>) this.Fields)
      service.Fields.Add((ServiceField) field.ObjectCopyDeep());
  }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    if (reader == null)
      throw new ArgumentNullException(nameof (reader));
    switch (member)
    {
      case "Id":
        this.Id = reader.ReadString();
        break;
      case "Name":
        this.Name = reader.ReadString();
        break;
      case "Description":
        this.Description = reader.ReadString();
        break;
      case "ServiceUrl":
        this.ServiceUrl = reader.ReadString();
        break;
      case "Fields":
        reader.AddObjects<ServiceField>((Func<ServiceField>) (() => new ServiceField()), (ICollection<ServiceField>) this.Fields);
        break;
      case "Version":
        this.Version = reader.ReadEnum<ODataVersion>();
        break;
      default:
        reader.Skip();
        break;
    }
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    if (writer == null)
      throw new ArgumentNullException();
    base.WriteJsonFields(writer);
    writer.Write("Id", this.Id);
    writer.Write("Name", this.Name);
    writer.Write("Description", this.Description);
    writer.Write("ServiceUrl", this.ServiceUrl);
    writer.Write<ServiceField>("Fields", (IEnumerable<ServiceField>) this.Fields);
    writer.Write("Version", (Enum) this.Version);
  }

  public object ObjectCopyDeep() => (object) new Service(this);
}
