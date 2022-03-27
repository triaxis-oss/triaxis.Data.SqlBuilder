namespace triaxis.Data.SqlBuilder;

using System.Reflection;

internal interface ITypeConverter
{
    object? Convert(object? value);
}

internal static class TypeConverter
{
    class TypeCodeConverter : ITypeConverter
    {
        private readonly TypeCode _tc;

        public TypeCodeConverter(TypeCode tc)
        {
            _tc = tc;
        }

        public object? Convert(object? value)
            => System.Convert.ChangeType(value, _tc);
    }

    class ChangeTypeConverter : ITypeConverter
    {
        private readonly Type _type;

        public ChangeTypeConverter(Type type)
        {
            _type = type;
        }

        public object? Convert(object? value)
            => System.Convert.ChangeType(value, _type);
    }

    class FromConverter<TFrom, TTo> : ITypeConverter
    {
        private readonly Converter<TFrom?, TTo> _converter;

        public FromConverter(MethodInfo method)
        {
            _converter = (Converter<TFrom?, TTo>)Delegate.CreateDelegate(
                typeof(Converter<TFrom?, TTo>), null, method);
        }

        public object? Convert(object? value)
            => _converter((TFrom?)value);
    }

    public static ITypeConverter Get(Type from, Type to)
    {
        var tc = Type.GetTypeCode(to);
        if (tc != TypeCode.Object)
            return new TypeCodeConverter(tc);

        if (to.GetMethod("From" + from.Name, new Type[] { from }) is MethodInfo mth)
        {
            if (to.IsAssignableFrom(mth.ReturnType))
            {
                var tConverter = typeof(FromConverter<,>).MakeGenericType(from, to);
                return (ITypeConverter)Activator.CreateInstance(tConverter, mth)!;
            }
        }

        return new ChangeTypeConverter(to);
    }

    public static SqlBuilder.AddToResultDelegate<T?> GetReader<T>(DbDataReader reader)
    {
        var from = reader.GetFieldType(0).UnwrapNullable();
        var to = typeof(T).UnwrapNullable();
        var tc = Get(from, to);
        return (IDataRecord rec, ref T? val) => val = rec.IsDBNull(0) ? default : (T?)tc.Convert(rec.GetValue(0));
    }
}
