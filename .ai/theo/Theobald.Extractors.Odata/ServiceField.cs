// Decompiled with JetBrains decompiler
// Type: Theobald.Extractors.Odata.ServiceField
// Assembly: Theobald.Extractors.Odata, Version=2025.7.11.11, Culture=neutral, PublicKeyToken=2e00b0608a9aff7e
// MVID: FE0C2532-16B1-4631-93B6-857FF05A96D4
// Assembly location: C:\Program Files\XtractUniversal\Theobald.Extractors.Odata.dll

using ERPConnect;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using Theobald.Extractors.Common;
using Theobald.Extractors.Conversion;
using Theobald.Json;

#nullable enable
namespace Theobald.Extractors.Odata;

public sealed class ServiceField : JsonSerializableObject, IDeepCopyable
{
  private static EdmType[] NotSupportedEdmTypes = new EdmType[1]
  {
    EdmType.Boolean
  };
  public static readonly Dictionary<string, EdmType> EdmTypeDic = new Dictionary<string, EdmType>()
  {
    {
      "Edm.Binary",
      EdmType.Binary
    },
    {
      "Edm.Boolean",
      EdmType.Boolean
    },
    {
      "Edm.Byte",
      EdmType.Byte
    },
    {
      "Edm.DateTime",
      EdmType.DateTime
    },
    {
      "Edm.DateTimeOffset",
      EdmType.DateTimeOffset
    },
    {
      "Edm.Decimal",
      EdmType.Decimal
    },
    {
      "Edm.Double",
      EdmType.Double
    },
    {
      "Edm.Int16",
      EdmType.Int16
    },
    {
      "Edm.Int32",
      EdmType.Int32
    },
    {
      "Edm.Int64",
      EdmType.Int64
    },
    {
      "Edm.Guid",
      EdmType.Guid
    },
    {
      "Edm.SByte",
      EdmType.SByte
    },
    {
      "Edm.String",
      EdmType.String
    },
    {
      "Edm.Time",
      EdmType.Time
    },
    {
      "Edm.Date",
      EdmType.Date
    },
    {
      "Edm.TimeOfDay",
      EdmType.TimeOfDay
    }
  };
  public static readonly Dictionary<EdmType, ResultType> EdmToResultTypeDic = new Dictionary<EdmType, ResultType>()
  {
    {
      EdmType.Binary,
      ResultType.ByteArrayLengthUnknown
    },
    {
      EdmType.Boolean,
      ResultType.StringLengthMax
    },
    {
      EdmType.Byte,
      ResultType.Byte
    },
    {
      EdmType.DateTime,
      ResultType.Date
    },
    {
      EdmType.DateTimeOffset,
      ResultType.Date
    },
    {
      EdmType.Decimal,
      ResultType.Decimal
    },
    {
      EdmType.Double,
      ResultType.Double
    },
    {
      EdmType.Int16,
      ResultType.Short
    },
    {
      EdmType.Int32,
      ResultType.Int
    },
    {
      EdmType.Int64,
      ResultType.Long
    },
    {
      EdmType.Guid,
      ResultType.StringLengthMax
    },
    {
      EdmType.SByte,
      ResultType.Short
    },
    {
      EdmType.String,
      ResultType.StringLengthUnknown
    },
    {
      EdmType.Time,
      ResultType.StringLengthMax
    },
    {
      EdmType.Date,
      ResultType.Date
    },
    {
      EdmType.TimeOfDay,
      ResultType.StringLengthMax
    }
  };

  public string Name { get; private set; } = string.Empty;

  public string Description { get; private set; } = string.Empty;

  public EdmType Type { get; private set; }

  public bool IsEdmTypeNotSupported
  {
    get => ((IEnumerable<EdmType>) ServiceField.NotSupportedEdmTypes).Contains<EdmType>(this.Type);
  }

  public uint Length { get; private set; }

  public bool IsSelected { get; set; } = true;

  public bool IsPrimaryKey { get; private set; }

  public uint DecimalCount { get; private set; }

  public uint DecimalScale { get; private set; }

  public ObservableCollection<Selection> Selections { get; } = new ObservableCollection<Selection>();

  internal ServiceField()
  {
  }

  public ServiceField(
    string name,
    string description,
    EdmType type,
    uint length,
    bool isPrimaryKey,
    uint decimalCount,
    uint decimalScale)
    : this(name, description, type, length, isPrimaryKey)
  {
    this.DecimalCount = decimalCount;
    this.DecimalScale = decimalScale;
  }

  public ServiceField(
    string name,
    string description,
    EdmType type,
    uint length,
    bool isPrimaryKey)
  {
    this.Name = name;
    this.Description = description;
    this.Type = type;
    this.Length = length;
    this.IsPrimaryKey = isPrimaryKey;
    if (!this.IsEdmTypeNotSupported)
      return;
    this.IsSelected = false;
  }

  private ServiceField(ServiceField copyFrom)
  {
    this.Name = copyFrom.Name;
    this.Description = copyFrom.Description;
    this.Type = copyFrom.Type;
    this.Length = copyFrom.Length;
    this.IsPrimaryKey = copyFrom.IsPrimaryKey;
    this.DecimalCount = copyFrom.DecimalCount;
    this.DecimalScale = copyFrom.DecimalScale;
    this.Selections = new ObservableCollection<Selection>(copyFrom.Selections.Select<Selection, Selection>((Func<Selection, Selection>) (selection => selection.CopyDeep<Selection>())));
  }

  private ResultType ResultType
  {
    get
    {
      int type = (int) this.Type;
      uint length = this.Length;
      return type != 12 ? ServiceField.EdmToResultTypeDic[this.Type] : (length != 0U ? ResultType.StringLengthMax : ResultType.StringLengthUnknown);
    }
  }

  internal ResultColumn ToResultColumn(ColumnNameConverter columnNameConverter, string namePrefix)
  {
    return ResultColumn.New(columnNameConverter, this.Name, this.Description, namePrefix, AbapType.CharacterString, checked ((int) this.Length), checked ((int) this.DecimalCount), this.IsPrimaryKey).With(this.ResultType);
  }

  public override string ToString()
  {
    return $"{this.Name}-{this.Description} ({this.Type} - {this.Length})";
  }

  private static string SelectionOptionToOdata(bool isInclude, SingleSelectionOption option)
  {
    if (isInclude)
    {
      switch (option)
      {
        case SingleSelectionOption.Equal:
          return "eq";
        case SingleSelectionOption.NotEqual:
          return "ne";
        case SingleSelectionOption.LessThan:
          return "lt";
        case SingleSelectionOption.GreaterThan:
          return "gt";
        case SingleSelectionOption.LessOrEqual:
          return "le";
        case SingleSelectionOption.GreaterOrEqual:
          return "ge";
      }
    }
    else
    {
      switch (option)
      {
        case SingleSelectionOption.Equal:
          return "ne";
        case SingleSelectionOption.NotEqual:
          return "eq";
        case SingleSelectionOption.LessThan:
          return "gt";
        case SingleSelectionOption.GreaterThan:
          return "lt";
        case SingleSelectionOption.LessOrEqual:
          return "ge";
        case SingleSelectionOption.GreaterOrEqual:
          return "le";
      }
    }
    throw new InvalidOperationException("Operator not expected " + option.ToString());
  }

  private string GetSelectionValue(string value)
  {
    switch (this.Type)
    {
      case EdmType.Boolean:
        return this.Type != EdmType.Boolean || bool.TryParse(value, out bool _) ? value.ToLower() : throw new FormatException($"Invalid value for filter on column {this.Name} ({this.Type}), which must match the following pattern: true or false");
      case EdmType.DateTime:
        if (this.Type == EdmType.DateTime && !DateTime.TryParseExact(value, "yyyy-MM-ddTHH:mm:ss", (IFormatProvider) CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime _))
          throw new FormatException($"Invalid value for filter on column {this.Name}({this.Type}), which must match the following pattern: datetime'yyyy-MM-dd:THH:mm:ss'");
        return $"datetime'{value}'";
      case EdmType.String:
        return $"'{value}'";
      default:
        return value;
    }
  }

  public string SelectionsToString()
  {
    if (this.Selections.Count < 1)
      return string.Empty;
    StringBuilder stringBuilder1 = new StringBuilder("(");
    foreach (Selection selection in (Collection<Selection>) this.Selections)
    {
      StringBuilder.AppendInterpolatedStringHandler interpolatedStringHandler;
      if (selection is SingleValueSelection singleValueSelection)
      {
        string selectionValue = this.GetSelectionValue(singleValueSelection.Value);
        string odata = ServiceField.SelectionOptionToOdata(selection.Include, singleValueSelection.Option);
        StringBuilder stringBuilder2 = stringBuilder1;
        StringBuilder stringBuilder3 = stringBuilder2;
        interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(2, 3, stringBuilder2);
        interpolatedStringHandler.AppendFormatted(this.Name);
        interpolatedStringHandler.AppendLiteral(" ");
        interpolatedStringHandler.AppendFormatted(odata);
        interpolatedStringHandler.AppendLiteral(" ");
        interpolatedStringHandler.AppendFormatted(selectionValue);
        ref StringBuilder.AppendInterpolatedStringHandler local = ref interpolatedStringHandler;
        stringBuilder3.Append(ref local);
      }
      else if (selection is ValueRangeSelection valueRangeSelection)
      {
        string selectionValue1 = this.GetSelectionValue(valueRangeSelection.LowerValue);
        string selectionValue2 = this.GetSelectionValue(valueRangeSelection.UpperValue);
        if (valueRangeSelection.Include)
        {
          StringBuilder stringBuilder4 = stringBuilder1;
          StringBuilder stringBuilder5 = stringBuilder4;
          interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(13, 4, stringBuilder4);
          interpolatedStringHandler.AppendFormatted(this.Name);
          interpolatedStringHandler.AppendLiteral(" ge ");
          interpolatedStringHandler.AppendFormatted(selectionValue1);
          interpolatedStringHandler.AppendLiteral(" and ");
          interpolatedStringHandler.AppendFormatted(this.Name);
          interpolatedStringHandler.AppendLiteral(" le ");
          interpolatedStringHandler.AppendFormatted(selectionValue2);
          ref StringBuilder.AppendInterpolatedStringHandler local = ref interpolatedStringHandler;
          stringBuilder5.Append(ref local);
        }
        else
        {
          StringBuilder stringBuilder6 = stringBuilder1;
          StringBuilder stringBuilder7 = stringBuilder6;
          interpolatedStringHandler = new StringBuilder.AppendInterpolatedStringHandler(12, 4, stringBuilder6);
          interpolatedStringHandler.AppendFormatted(this.Name);
          interpolatedStringHandler.AppendLiteral(" lt ");
          interpolatedStringHandler.AppendFormatted(selectionValue1);
          interpolatedStringHandler.AppendLiteral(" or ");
          interpolatedStringHandler.AppendFormatted(this.Name);
          interpolatedStringHandler.AppendLiteral(" gt ");
          interpolatedStringHandler.AppendFormatted(selectionValue2);
          ref StringBuilder.AppendInterpolatedStringHandler local = ref interpolatedStringHandler;
          stringBuilder7.Append(ref local);
        }
      }
      if (this.Selections.Last<Selection>() != selection)
        stringBuilder1.Append(" or ");
    }
    stringBuilder1.Append(")");
    return stringBuilder1.ToString();
  }

  public override void ReadJsonProperty(JsonReader reader, string member)
  {
    if (reader == null)
      throw new ArgumentNullException(nameof (reader));
    if (member != null)
    {
      switch (member.Length)
      {
        case 4:
          switch (member[0])
          {
            case 'N':
              if (member == "Name")
              {
                this.Name = reader.ReadString();
                return;
              }
              break;
            case 'T':
              if (member == "Type")
              {
                this.Type = reader.ReadEnum<EdmType>();
                return;
              }
              break;
          }
          break;
        case 6:
          if (member == "Length")
          {
            this.Length = (uint) reader.ReadUInt16();
            return;
          }
          break;
        case 10:
          switch (member[0])
          {
            case 'I':
              if (member == "IsSelected")
              {
                this.IsSelected = reader.ReadBoolean();
                return;
              }
              break;
            case 'S':
              if (member == "Selections")
              {
                reader.AddObjects<Selection>((Func<Selection>) (() => (Selection) new SingleValueSelection()), (ICollection<Selection>) this.Selections);
                return;
              }
              break;
          }
          break;
        case 11:
          if (member == "Description")
          {
            this.Description = reader.ReadString();
            return;
          }
          break;
        case 12:
          switch (member[7])
          {
            case 'C':
              if (member == "DecimalCount")
              {
                this.DecimalCount = checked ((uint) reader.ReadInt32());
                return;
              }
              break;
            case 'S':
              if (member == "DecimalScale")
              {
                this.DecimalScale = checked ((uint) reader.ReadInt32());
                return;
              }
              break;
            case 'r':
              if (member == "IsPrimaryKey")
              {
                this.IsPrimaryKey = reader.ReadBoolean();
                return;
              }
              break;
          }
          break;
        case 15:
          if (member == "RangeSelections")
          {
            reader.AddObjects<Selection>((Func<Selection>) (() => (Selection) new ValueRangeSelection()), (ICollection<Selection>) this.Selections);
            return;
          }
          break;
      }
    }
    reader.Skip();
  }

  public override void WriteJsonFields(JsonWriter writer)
  {
    if (writer == null)
      throw new ArgumentNullException(nameof (writer));
    base.WriteJsonFields(writer);
    writer.Write("Name", this.Name);
    writer.Write("Description", this.Description);
    writer.Write("Type", (Enum) this.Type);
    writer.Write("Length", new long?((long) this.Length));
    writer.Write("IsSelected", new bool?(this.IsSelected));
    writer.Write("IsPrimaryKey", new bool?(this.IsPrimaryKey));
    writer.Write("DecimalCount", new long?((long) this.DecimalCount));
    writer.Write("DecimalScale", new long?((long) this.DecimalScale));
    if (this.Selections.Count <= 0)
      return;
    writer.Write<Selection>("Selections", this.Selections.Where<Selection>((Func<Selection, bool>) (s => s is SingleValueSelection)));
    writer.Write<Selection>("RangeSelections", this.Selections.Where<Selection>((Func<Selection, bool>) (s => s is ValueRangeSelection)));
  }

  public object ObjectCopyDeep() => (object) new ServiceField(this);
}
