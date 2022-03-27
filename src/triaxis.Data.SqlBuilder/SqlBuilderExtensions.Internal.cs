namespace triaxis.Data.SqlBuilder;

public static partial class SqlBuilderExtensions
{
    internal static IProperty GetEntityProperty<T>(this Expression<Func<T, object?>> selector, DbContext context)
        => context.GetEntityType<T>().GetProperty(selector.GetMemberAccess().Name) ?? throw new ArgumentException("Property not mapped", nameof(selector));

    internal static IEntityType GetEntityType<T>(this DbContext context)
        => context.Model.FindEntityType(typeof(T)) ?? throw new ArgumentException("Model not mapped", nameof(T));
}
