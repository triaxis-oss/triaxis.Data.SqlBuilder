namespace triaxis.Data.SqlBuilder;

using System.Reflection;
using triaxis.Reflection;

internal class SqlMapper<T>
        where T : new()
{
    private readonly IPropertySetter[] _setters;
    private readonly ITypeConverter[] _converters;

    public SqlMapper(DbDataReader reader, IEntityType? entityType = null)
    {
        _setters = new IPropertySetter[reader.FieldCount];
        _converters = new ITypeConverter[_setters.Length];
        Dictionary<string, PropertyInfo> properties;

        if (entityType is not null)
        {
            // map to entity
            properties = entityType.GetProperties()
                .Where(prop => prop.PropertyInfo is not null)
                .ToDictionary(prop => prop.GetColumnBaseName(), prop => prop.PropertyInfo!, StringComparer.OrdinalIgnoreCase);
        }
        else
        {
            // map to object
            properties = typeof(T).GetProperties()
                .ToDictionary(prop => prop.Name, StringComparer.OrdinalIgnoreCase);
        }

        for (int i = 0; i < _setters.Length; i++)
        {
            if (!properties.TryGetValue(reader.GetName(i), out var pi))
            {
                continue;
            }
            _setters[i] = pi.GetSetter();
            var propType = pi.PropertyType;
            propType = Nullable.GetUnderlyingType(propType) ?? propType;
            var fieldType = reader.GetFieldType(i);
            if (!propType.IsAssignableFrom(fieldType))
            {
                _converters[i] = TypeConverter.Get(fieldType, propType);
            }
        }
    }

    public void Read(IDataRecord rdr, ref T? record)
    {
        record ??= new();
        ReadNotNull(rdr, ref record);
    }

    public void ReadNotNull(IDataRecord rdr, ref T record)
    {
        for (int i = 0; i < _setters.Length; i++)
        {
            if (_setters[i] == null)
                continue;
            object? val = rdr.GetValue(i);
            if (val == DBNull.Value)
                val = null;
            else if (val != null && _converters[i] != null)
                val = _converters[i].Convert(val);
            _setters[i].Set(record, val);
        }
    }

    public void AddToList(IDataRecord rdr, ref List<T> list)
    {
        T record = new();
        ReadNotNull(rdr, ref record);
        list.Add(record);
    }
}
