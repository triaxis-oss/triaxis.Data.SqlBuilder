namespace triaxis.Data.SqlBuilder;

using ContextStates;

partial class SqlBuilderExtensions
{
    /// <summary>
    /// Begins a WHERE condition on a property-mapped column of a mapped entity
    /// </summary>
    /// <typeparam name="T">The mapped type from which the property is selected</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="selector">An expression identifying the property</param>
    public static Context<T, Where> Where<T>(this IContext<T, Condition> context, Expression<Func<T, object?>> selector)
    {
        var sql = context.Builder;
        sql.Where(selector.GetEntityProperty(sql.Context).GetColumnBaseName());
        return new(sql);
    }

    /// <summary>
    /// Appends a subquery as a WHERE condition, preserving the entity context of the statement
    /// </summary>
    /// <typeparam name="T">The mapped entity type</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="conditionBuilder">A delegate for emitting the subquery</param>
    public static Context<T, Condition> Where<T>(this IContext<T, Condition> context, Action<SqlBuilder> conditionBuilder)
    {
        var sql = context.Builder;
        sql.Where(conditionBuilder);
        return new(sql);
    }
}
