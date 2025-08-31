// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.OdpOData.TypeMappings
// Assembly: Theobald.Extractors.OdpOData, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=0d3889221f26497f
// MVID: 67F12262-922B-4974-998F-CC8914B0DD3F
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.OdpOData.dll

using System;
using Theobald.Extractors.Common;

#nullable enable
namespace Theobald.Extractors.OdpOData;

internal static class TypeMappings
{
  public static (ResultType, int) TranslateTypeInfo(Field field)
  {
    switch (field.Type)
    {
      case EdmType.Binary:
        int? fixedLength1 = field.FixedLength;
        if (fixedLength1.HasValue)
          return (ResultType.ByteArrayLengthExact, fixedLength1.GetValueOrDefault());
        int? maxLength1 = field.MaxLength;
        return maxLength1.HasValue ? (ResultType.ByteArrayLengthMax, maxLength1.GetValueOrDefault()) : (ResultType.ByteArrayLengthUnknown, 0);
      case EdmType.Boolean:
        throw new NotSupportedException("Booleans are not yet supported");
      case EdmType.Byte:
        return (ResultType.Byte, 0);
      case EdmType.DateTime:
        return (ResultType.ConvertedTimeStampTicks, 0);
      case EdmType.DateTimeOffset:
        return (ResultType.ConvertedTimeStampTicks, 0);
      case EdmType.Decimal:
        return (ResultType.Decimal, field.Precision.GetValueOrDefault());
      case EdmType.Double:
        return (ResultType.Double, 0);
      case EdmType.Single:
        return (ResultType.Double, 0);
      case EdmType.Guid:
        throw new NotSupportedException("GUIDs are not yet supported");
      case EdmType.Int16:
        return (ResultType.Short, 0);
      case EdmType.Int32:
        return (ResultType.Int, 0);
      case EdmType.Int64:
        return (ResultType.Long, 0);
      case EdmType.SByte:
        throw new NotSupportedException("SBytes are not yet supported");
      case EdmType.String:
        int? fixedLength2 = field.FixedLength;
        if (fixedLength2.HasValue)
          return (ResultType.StringLengthMax, fixedLength2.GetValueOrDefault());
        int? maxLength2 = field.MaxLength;
        return maxLength2.HasValue ? (ResultType.StringLengthMax, maxLength2.GetValueOrDefault()) : (ResultType.StringLengthUnknown, 0);
      case EdmType.Time:
        return (ResultType.ConvertedTime, 0);
      default:
        throw new ArgumentOutOfRangeException("Type", $"Type `{field.Type}` is unknown");
    }
  }
}
