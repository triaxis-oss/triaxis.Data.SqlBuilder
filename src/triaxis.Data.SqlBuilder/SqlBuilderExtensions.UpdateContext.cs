namespace triaxis.Data.SqlBuilder;

using ContextStates;

partial class SqlBuilderExtensions
{
    /// <summary>
    /// Starts an assignment to the specified property-mapped column
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to update</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="selector">An expression identifying the property</param>
    public static Context<T, Set> Set<T>(this IContext<T, Update> context, Expression<Func<T, object?>> selector)
    {
        var sql = context.Builder;
        sql.Set(selector.GetEntityProperty(sql.Context).GetColumnBaseName());
        return new(sql);
    }

    /// <summary>
    /// Assigns a literal value to the specified property-mapped column
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to update</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="selector">An expression identifying the property</param>
    /// <param name="value">Literal value to assign</param>
    public static Context<T, Update> Set<T>(this IContext<T, Update> context, Expression<Func<T, object?>> selector, object? value)
    {
        var sql = context.Builder;
        sql.Set(selector.GetEntityProperty(sql.Context).GetColumnBaseName());
        sql.AppendValue(value);
        return new(sql);
    }
}
