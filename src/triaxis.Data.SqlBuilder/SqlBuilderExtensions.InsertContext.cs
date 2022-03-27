namespace triaxis.Data.SqlBuilder;

using ContextStates;

partial class SqlBuilderExtensions
{
    /// <summary>
    /// Appends a single row of values from the specified entity to the INSERT statement
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to insert</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="value">The actual entity from which values are INSERTed</param>
    public static Context<T, Insert> Values<T>(this Context<T, Insert> context, T value)
        where T : notnull
    {
        context.Builder.Values(value);
        return context;
    }

    /// <summary>
    /// Appends multiple rows of values from the specified entities to the INSERT statement
    /// </summary>
    /// <typeparam name="T">Type of the mapped entities to insert</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    /// <param name="values">The actual entities from which values are INSERTed</param>
    public static Context<T, Insert> ValuesList<T>(this Context<T, Insert> context, IEnumerable<T> values)
        where T : notnull
    {
        context.Builder.Values(values);
        return context;
    }

    /// <summary>
    /// Turns the current INSERT statement into an UPSERT, replacing existing values in case a row is already found
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Context<T, PostInsert> OnDuplicateKeyUpdate<T>(this Context<T, Insert> context)
    {
        var builder = context.Builder;
        builder.OnDuplicateKeyUpdate();
        return new(builder);
    }
}
