namespace triaxis.Data.SqlBuilder;

using ContextStates;

/// <summary>
/// Extensions for working with <see cref="SqlBuilder" /> in a type-safe manner
/// </summary>
public static partial class SqlBuilderExtensions
{
    /// <summary>
    /// Starts a SELECT statement for the specified entity type, including all its columns
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to select</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Context<T, Select> Select<T>(this IContext context)
        => context.Builder.Select<T>();

    /// <summary>
    /// Begins a new INSERT statement for the specified entity type
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to insert</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Context<T, Insert> Insert<T>(this IContext context)
        => context.Builder.Insert<T>();

    /// <summary>
    /// Starts a new UPDATE statement for the specified entity type
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to update</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Context<T, Update> Update<T>(this IContext context)
        => context.Builder.Update<T>();

    /// <summary>
    /// Starts a new DELETE statement for the specified entity type
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to delete</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Context<T, Delete> Delete<T>(this IContext context)
        => context.Builder.Delete<T>();
}
