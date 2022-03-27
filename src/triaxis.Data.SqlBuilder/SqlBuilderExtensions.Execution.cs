namespace triaxis.Data.SqlBuilder;

using ContextStates;

partial class SqlBuilderExtensions
{
    /// <summary>
    /// Executes the query and returns the list of mapped entities
    /// </summary>
    /// <typeparam name="T">Type of the mapped entities to select</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Task<List<T>> QueryAsync<T>(this IEntityContext<T> context) where T : new()
        => context.Builder.ExecuteAsync(new List<T>(), rdr => new SqlMapper<T>(rdr, context.Builder.EntityType).AddToList);

    /// <summary>
    /// Executes the query and returns the first found entity from the generated SQL; throws if no entity is found
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to select</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Task<T> QueryFirstAsync<T>(this IEntityContext<T> context) where T : new()
        => context.Builder.Limit(1).ExecuteAsync(new T(), rdr => new SqlMapper<T>(rdr, context.Builder.EntityType).ReadNotNull, SqlBuilder.ExecuteOptions.NoDefault);

    /// <summary>
    /// Executes the query and returns the first found entity from the generated SQL; returns <see langword="null" /> if no entity is found
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to select</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Task<T?> QueryFirstOrDefaultAsync<T>(this IEntityContext<T> context) where T : new()
        => context.Builder.Limit(1).ExecuteAsync<T?>(default, rdr => new SqlMapper<T>(rdr, context.Builder.EntityType).Read, SqlBuilder.ExecuteOptions.None);

    /// <summary>
    /// Executes the query and returns the only found entity from the generated SQL; throws if no entity is found, or if multiple are found
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to select</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Task<T> QuerySingleAsync<T>(this IEntityContext<T> context) where T : new()
        => context.Builder.Limit(2).ExecuteAsync(new T(), rdr => new SqlMapper<T>(rdr, context.Builder.EntityType).ReadNotNull, SqlBuilder.ExecuteOptions.Single);

    /// <summary>
    /// Executes the query and returns the only found entity from the generated SQL; returns <see langword="null" /> if no entity is found, throws if multiple are found
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to select</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Task<T?> QuerySingleOrDefaultAsync<T>(this IEntityContext<T> context) where T : new()
        => context.Builder.Limit(2).ExecuteAsync<T?>(default, rdr => new SqlMapper<T>(rdr, context.Builder.EntityType).Read, SqlBuilder.ExecuteOptions.Single | SqlBuilder.ExecuteOptions.NoDefault);

    /// <summary>
    /// Executes the query returning a single scalar value; returns <see langword="null" /> if no value is returned, throws if multiple values are returned
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to select</typeparam>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static Task<T?> ExecuteScalarAsync<T>(this IContext context) where T : new()
        => context.Builder.ExecuteAsync<T?>(default, TypeConverter.GetReader<T>, SqlBuilder.ExecuteOptions.Single);

    /// <summary>
    /// Executes the statement, returning the total number of affected rows
    /// </summary>
    /// <param name="context">The <see cref="SqlBuilder" /> context</param>
    public static async Task<int> ExecuteAsync(this IContext context)
    {
        int numAffected = 0;
        await context.Builder.ExecuteAsync(0, rdr => { numAffected += rdr.RecordsAffected; return null; });
        return numAffected;
    }
}
