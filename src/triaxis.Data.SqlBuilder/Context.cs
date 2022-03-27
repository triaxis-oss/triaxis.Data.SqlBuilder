namespace triaxis.Data.SqlBuilder;

using ContextStates;

/// <summary>
/// Represents any <see cref="SqlBuilder" /> context
/// </summary>
public interface IContext
{
    /// <summary>
    /// Gets the underlying <see cref="SqlBuilder" />
    /// </summary>
    public SqlBuilder Builder { get; }
}

/// <summary>
/// Represents an <see cref="SqlBuilder" /> context in the specified state
/// </summary>
public interface IStateContext<out TState> : IContext
    where TState : Any
{
}

/// <summary>
/// Represents an <see cref="SqlBuilder" /> context querying the specified entity
/// </summary>
public interface IEntityContext<TEntity> : IContext
{
}

/// <summary>
/// Represents an <see cref="SqlBuilder" /> context querying the specified entity and being in the specified state
/// </summary>
public interface IContext<TEntity, out TState> : IEntityContext<TEntity>, IStateContext<TState>
    where TState : Any
{
}

/// <summary>
/// Represents an <see cref="SqlBuilder" /> context querying the specified entity and being in the specified state
/// </summary>
public struct Context<TEntity, TState> : IContext<TEntity, TState>
    where TState : Any
{
    internal Context(SqlBuilder builder) { Builder = builder; }
    /// <inheritdoc />
    public SqlBuilder Builder { get; }
}
