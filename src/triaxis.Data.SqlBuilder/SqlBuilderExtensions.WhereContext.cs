namespace triaxis.Data.SqlBuilder;

using ContextStates;

partial class SqlBuilderExtensions
{
    /// <summary>
    /// Compares the last specified WHERE column with a literal value using the specified operator
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="value">Literal value to compare against</param>
    /// <param name="operator">Comparison operator to use</param>
    public static Context<T, Condition> Op<T>(this IContext<T, Where> context, object? value, string @operator)
    {
        var sql = context.Builder;
        sql.WhereValue(value, @operator);
        return new(sql);
    }

    /// <summary>
    /// Checks whether the last specified WHERE column is NULL
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Context<T, Condition> IsNull<T>(this IContext<T, Where> context)
        => context.Op(null, "=");

    /// <summary>
    /// Checks whether the last specified WHERE column is NOT NULL
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Context<T, Condition> IsNotNull<T>(this IContext<T, Where> context)
        => context.Op(null, "<>");

    /// <summary>
    /// Checks whether the last specified WHERE column is equal to the specified literal value
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="value">Literal value to compare against</param>
    public static Context<T, Condition> Eq<T>(this IContext<T, Where> context, object? value)
        => context.Op(value, "=");

    /// <summary>
    /// Checks whether the last specified WHERE column is not equal to the specified literal value
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="value">Literal value to compare against</param>
    public static Context<T, Condition> Ne<T>(this IContext<T, Where> context, object? value)
        => context.Op(value, "<>");

    /// <summary>
    /// Checks whether the last specified WHERE column is less than the specified literal value
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="value">Literal value to compare against</param>
    public static Context<T, Condition> Lt<T>(this IContext<T, Where> context, object? value)
        => context.Op(value, "<");

    /// <summary>
    /// Checks whether the last specified WHERE column is greater than the specified literal value
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="value">Literal value to compare against</param>
    public static Context<T, Condition> Gt<T>(this IContext<T, Where> context, object? value)
        => context.Op(value, ">");

    /// <summary>
    /// Checks whether the last specified WHERE column is less than or equal to the specified literal value
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="value">Literal value to compare against</param>
    public static Context<T, Condition> Le<T>(this IContext<T, Where> context, object? value)
        => context.Op(value, "<=");

    /// <summary>
    /// Checks whether the last specified WHERE column is greater than or equal to the specified literal value
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="value">Literal value to compare against</param>
    public static Context<T, Condition> Ge<T>(this IContext<T, Where> context, object? value)
        => context.Op(value, ">=");

    /// <summary>
    /// Checks whether the last specified WHERE column is between the specified literal values
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <typeparam name="TValue">Type of the values</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="left">Lower bound of the BETWEEN clause (inclusive)</param>
    /// <param name="right">Upper bound of the BETWEEN clause (inclusive!)</param>
    public static Context<T, Condition> Between<T, TValue>(this IContext<T, Where> context, TValue left, TValue right)
    {
        var sql = context.Builder;
        sql.WhereBetween(left, right);
        return new(sql);
    }

    /// <summary>
    /// Compares the last specified WHERE column with an arbitrary SQL expression using the specified operator
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="expression">A delegate for emitting the expression or subquery</param>
    /// <param name="operator">Comparison operator to use</param>
    public static Context<T, Condition> Sql<T>(this IContext<T, Where> context, Action<SqlBuilder> expression, string @operator = "=")
    {
        var sql = context.Builder;
        sql.Where(expression, @operator);
        return new(sql);
    }
}
