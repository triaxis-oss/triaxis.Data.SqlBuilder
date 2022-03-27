namespace triaxis.Data.SqlBuilder;

static class ReflectionExtensions
{
    public static Type UnwrapNullable(this Type type)
        => Nullable.GetUnderlyingType(type) ?? type;
}
