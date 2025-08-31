// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.NamedRef
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;

#nullable enable
namespace Theobald.Extractors.OdpOData;

public readonly struct NamedRef
{
  public string? Namespace { get; init; }

  public required string Name { get; init; }

  private static bool IsNullOrEmptyOrWhitesSpace(string value)
  {
    return string.IsNullOrEmpty(value) || string.IsNullOrWhiteSpace(value);
  }

  public static NamedRef Simple(string name)
  {
    return new NamedRef() { Name = name };
  }

  public static NamedRef Parse(string raw)
  {
    string[] parts = !string.IsNullOrEmpty(raw) ? raw.Split('.') : throw new ArgumentNullException(nameof (raw));
    switch (parts.Length)
    {
      case 1:
        return NamedRef.FromNameOnly(raw, parts);
      case 2:
        return NamedRef.FromNameAndNamespace(raw, parts);
      default:
        throw new NamedRefParseException(raw);
    }
  }

  private static NamedRef FromNameAndNamespace(string raw, string[] parts)
  {
    string part1 = parts[0];
    string part2 = parts[1];
    return !NamedRef.IsNullOrEmptyOrWhitesSpace(part1) && !NamedRef.IsNullOrEmptyOrWhitesSpace(part2) ? new NamedRef()
    {
      Namespace = part1,
      Name = part2
    } : throw new NamedRefParseException(raw);
  }

  private static NamedRef FromNameOnly(string raw, string[] parts)
  {
    string part = parts[0];
    return !NamedRef.IsNullOrEmptyOrWhitesSpace(part) ? new NamedRef()
    {
      Name = part
    } : throw new NamedRefParseException(raw);
  }

  public override string ToString()
  {
    return this.Namespace != null ? $"{this.Namespace}.{this.Name}" : this.Name;
  }

  public bool Equals(NamedRef other)
  {
    return this.Namespace == other.Namespace && this.Name == other.Name;
  }

  public override int GetHashCode() => HashCode.Combine<string, string>(this.Namespace, this.Name);
}
