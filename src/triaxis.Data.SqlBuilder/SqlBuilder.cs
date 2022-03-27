namespace triaxis.Data.SqlBuilder;

using System.Collections;
using System.Globalization;
using ContextStates;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

/// <summary>
/// Helper for building SQL queries using fluent syntax
/// </summary>
public partial class SqlBuilder
{
    private static readonly CultureInfo _invar = CultureInfo.InvariantCulture;

    private enum State
    {
        Start,
        Select,
        From,
        Insert,
        Values,
        Update,
        Set,
        Delete,
        With,
        Join,
        JoinOn,
        Where,
        WhereOr,
        OrderBy,
        Options,
        WhereCondition,
    }

    private readonly DbContext _context;
    private readonly SqlSyntax _syntax;
    private StringBuilder _sql;
    private int _lastStart = 0;
    private int _columnsStart, _columnsEnd;
    private IEntityType? _entityType;
    private State _state;

    /// <summary>
    /// Creates a new instance of a <see cref="SqlBuilder" />
    /// </summary>
    /// <param name="context">The EF Core <see cref="DbContext" /> used to resolve mapped names</param>
    /// <param name="syntax">Optionally explicitly specified SQL dialect, autodetected from <see paramref="context" /> if <see langword="null" /></param>
    public SqlBuilder(DbContext context, SqlSyntax? syntax = null)
    {
        _context = context;
        _syntax = syntax ?? SqlSyntax.FromContext(context);
        _sql = new();
    }

    SqlBuilder NextCommand()
    {
        if (_state != State.Start)
        {
            _sql.Append(_syntax.CommandSeparator);
            _lastStart = _sql.Length;
            _state = State.Start;
            _columnsStart = _columnsEnd = 0;
        }
        return this;
    }

    /// <summary>
    /// Gets the associated <see cref="DbContext" />
    /// </summary>
    public DbContext Context => _context;

    /// <summary>
    /// Gets the <see cref="SqlSyntax" /> used by the builder
    /// </summary>
    public SqlSyntax Syntax => _syntax;

    /// <summary>
    /// Gets the <see cref="IEntityType" /> used in the current statement
    /// </summary>
    internal IEntityType? EntityType => _entityType;


    #region High-level helpers

    private void BeginSelect()
    {
        if (_state != State.Select)
        {
            if (_state == State.Insert)
            {
                // support INSERT ... SELECT
                AppendSql(" ");
            }
            else
            {
                NextCommand();
            }
            AppendSql("SELECT ");
            _state = State.Select;
            if (_columnsStart == 0)
            {
                _columnsStart = _sql.Length;
            }
        }
        else
        {
            AppendSql(",");
        }
    }

    /// <summary>
    /// Appends a column to a SELECT statement, starting the statement if required
    /// </summary>
    /// <param name="column">Name of the column to add to the SELECT, include optional table reference</param>
    /// <param name="alias">Optional alias of the column</param>
    public SqlBuilder Select(string column, string? alias = null)
    {
        BeginSelect();
        AppendExpression(column);
        if (alias != null)
        {
            AppendSql(" AS ");
            AppendIdentifier(alias);
        }
        return this;
    }

    /// <summary>
    /// Appends a calculated expression as a column to a SELECT statement, starting the statement if required
    /// </summary>
    /// <param name="expression">A <see cref="FormattableString" /> representing any SQL expression, interpolated values are safely processed into literals</param>
    /// <param name="alias">Optional alias of the column</param>
    public SqlBuilder SelectExpression(FormattableString expression, string? alias = null)
    {
        BeginSelect();
        AppendInterpolated(expression);
        if (alias != null)
        {
            AppendSql(" AS ");
            AppendIdentifier(alias);
        }
        return this;
    }

    /// <summary>
    /// Appends the specified columns to a SELECT statement, starting the statement if required
    /// </summary>
    /// <param name="columns">Names of columns to append</param>
    public SqlBuilder Select(params string[] columns)
    {
        foreach (var column in columns)
            Select(column);
        return this;
    }

    /// <summary>
    /// Starts a SELECT statement for the specified entity type, including all its columns
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to select</typeparam>
    public Context<T, Select> Select<T>()
    {
        Select(_context.GetEntityType<T>());
        return new(this);
    }

    /// <summary>
    /// Starts a SELECT statement for the specified entity type, including only the specified column
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to select</typeparam>
    /// <param name="property">An expression identifying the property to select</param>
    public Context<T, Select> Select<T>(Expression<Func<T, object?>> property)
    {
        _entityType = _context.Model.FindEntityType(typeof(T)) ?? throw new ArgumentException("Model not mapped", nameof(T));
        var prop = _entityType.GetProperty(property.GetMemberAccess().Name);
        Select(prop.GetColumnBaseName());
        return new(this);
    }

    internal SqlBuilder Select(IEntityType type)
    {
        foreach (var prop in type.GetProperties())
            Select(prop.GetColumnBaseName());
        return this;
    }

    /// <summary>
    /// Appends a SELECT statement selecting the last inserted value
    /// </summary>
    public SqlBuilder SelectLastInsertID()
    {
        NextCommand();
        AppendSql("SELECT ");
        AppendSql(_syntax.LastIdentityExpression);
        _state = State.Options;
        return this;
    }

    /// <summary>
    /// Appends the FROM clause to a SELECT statement.
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to select</typeparam>
    /// <param name="alias">Optional table alias</param>
    /// <remark>Note this is rarely needed to be done explicitly, as the FROM clause is added automatically before starting the WHERE clause</remark>
    public Context<T, Select> From<T>(string? alias = null)
    {
        From(_context.GetEntityType<T>(), alias);
        return new(this);
    }

    internal SqlBuilder From(IEntityType type, string? alias = null)
    {
        if (_state != State.Select)
        {
            throw new InvalidOperationException();
        }

        if (_columnsEnd == 0)
        {
            _columnsEnd = _sql.Length;
        }

        AppendInterpolated($" FROM {type}");
        if (alias != null)
        {
            AppendSql(" ");
            AppendIdentifier(alias);
        }
        _state = State.From;
        _entityType = type;
        return this;
    }

    /// <summary>
    /// Begins a new INSERT statement for the specified entity type
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to insert</typeparam>
    /// <param name="ignore">If <see langword="true" />, conflicting rows will not cause the statement to fail but will be silently skipped</param>
    /// <param name="select">Optional sub-select providing the values to be inserted</param>
    public Context<T, Insert> Insert<T>(bool ignore = false, Action<SqlBuilder>? select = null)
    {
        Insert(_context.GetEntityType<T>(), ignore, select);
        return new(this);
    }

    internal SqlBuilder Insert(IEntityType type, bool ignore = false, Action<SqlBuilder>? select = null)
    {
        NextCommand();
        AppendSql("INSERT ");
        if (ignore)
        {
            AppendSql("IGNORE ");
        }
        AppendSql("INTO ");
        AppendLiteral(type);
        AppendSql(" (").AppendColumns(type).AppendSql(")");

        if (select == null)
        {
            _state = State.Insert;
        }
        else
        {
            AppendSql(" ");
            _state = State.Start;
            select(this);
            _state = State.Options;
        }
        _entityType = type;
        return this;
    }

    /// <summary>
    /// Appends the VALUES for multiple rows to be INSERTed
    /// </summary>
    /// <typeparam name="T">Type of the mapped entities to insert</typeparam>
    /// <param name="values">The actual entities to be INSERTed</param>
    public SqlBuilder Values<T>(IEnumerable<T> values)
        where T : notnull
    {
        foreach (var row in values)
            Values(row);
        return this;
    }

    /// <summary>
    /// Appends the VALUES for a single row to be INSERTed
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to insert</typeparam>
    /// <param name="row">The actual entity from which values are INSERTed</param>
    /// <exception cref="InvalidOperationException">If the entity type <see typeparamref="T" /> does not match the type of the <see methodref="Insert{T}" />.</exception>
    public SqlBuilder Values<T>(T row)
        where T : notnull
    {
        if (_entityType is null || !_entityType.ClrType.IsAssignableFrom(typeof(T)))
        {
            throw new InvalidOperationException();
        }

        switch (_state)
        {
            case State.Insert:
                _state = State.Values;
                AppendSql(" VALUES (");
                break;

            case State.Values:
                AppendSql(",(");
                break;

            default:
                throw new InvalidOperationException();
        }

        AppendValues(_entityType, row);
        AppendSql(")");
        return this;
    }

    private void BeginJoin(string type)
    {
        if (_state == State.Select)
        {
            From(_entityType ?? throw new InvalidOperationException());
        }

        if (_state != State.From && _state != State.Join && _state != State.JoinOn)
        {
            throw new InvalidOperationException();
        }

        AppendSql("\n");
        AppendSql(type);
        AppendSql(" ");
    }

    /// <summary>
    /// Joins a subquery to the existing statement, using a LEFT JOIN
    /// </summary>
    /// <param name="subQuery">A delegate for emitting the subquery</param>
    /// <param name="alias">Alias of the subquery for referencing later in the statement</param>
    public SqlBuilder LeftJoin(Action<SqlBuilder> subQuery, string alias)
        => Join("LEFT JOIN", subQuery, alias);

    /// <summary>
    /// Joins the specified entity to the existing statement, using a LEFT JOIN
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to join</typeparam>
    /// <param name="alias">Alias of the subquery for referencing later in the statement</param>
    public SqlBuilder LeftJoin<T>(string alias)
        => Join("LEFT JOIN", _context.GetEntityType<T>(), alias);

    /// <summary>
    /// Joins a subquery to the existing statement, using an INNER JOIN
    /// </summary>
    /// <param name="subQuery">A delegate for emitting the subquery</param>
    /// <param name="alias">Alias of the subquery for referencing later in the statement</param>
    public SqlBuilder InnerJoin(Action<SqlBuilder> subQuery, string alias)
        => Join("INNER JOIN", subQuery, alias);

    /// <summary>
    /// Joins the specified entity to the existing statement, using an INNER JOIN
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to join</typeparam>
    /// <param name="alias">Alias of the subquery for referencing later in the statement</param>
    public SqlBuilder InnerJoin<T>(string alias)
        => Join("INNER JOIN", _context.GetEntityType<T>(), alias);

    private SqlBuilder Join(string joinType, Action<SqlBuilder> subQuery, string alias)
    {
        BeginJoin(joinType);
        AppendSql("(");
        _state = State.Start;
        subQuery(this);
        AppendSql(") ");
        AppendIdentifier(alias);
        _state = State.Join;
        return this;
    }

    private SqlBuilder Join(string joinType, IEntityType type, string alias)
    {
        BeginJoin(joinType);
        AppendLiteral(type);
        AppendSql(" ");
        AppendIdentifier(alias);
        _state = State.Join;
        return this;
    }

    private SqlBuilder AppendJoinOn()
    {
        switch (_state)
        {
            case State.Join:
                _state = State.JoinOn;
                return AppendSql(" ON ");
            case State.JoinOn:
                return AppendSql(" AND ");
            default:
                throw new InvalidOperationException();
        }
    }

    /// <summary>
    /// Appends an ON condition to the last JOIN clause, comparing two columns
    /// </summary>
    /// <param name="col1">First column to compare</param>
    /// <param name="col2">Second column to compare</param>
    /// <param name="op">Comparison operator</param>
    public SqlBuilder JoinOn(string col1, string col2, string op = "=")
    {
        return AppendJoinOn().AppendExpression(col1).AppendSql(op).AppendExpression(col2);
    }

    /// <summary>
    /// Appends an ON condition to the last JOIN clause, comparing a column against an expression or a subquery
    /// </summary>
    /// <param name="col">The column to compare</param>
    /// <param name="subQuery">A delegate for emitting the subquery</param>
    /// <param name="op">Comparison operator</param>
    public SqlBuilder JoinOn(string col, Action<SqlBuilder> subQuery, string op = "=")
    {
        AppendJoinOn().AppendExpression(col).AppendSql(op).AppendSql("(");
        _state = State.Start;
        subQuery(this);
        _state = State.JoinOn;
        AppendSql(")");
        return this;
    }

    /// <summary>
    /// Turns the last INSERT into an UPSERT operation, by updating the column in case an existing row is found
    /// </summary>
    public SqlBuilder OnDuplicateKeyUpdate()
    {
        if (_state != State.Values && _state != State.Options)
        {
            throw new InvalidOperationException();
        }

        if (_entityType == null)
        {
            throw new InvalidOperationException();
        }

        _state = State.Options;
        _syntax.OnDuplicateKeyUpdate(_sql,
            _entityType.GetProperties().Where(p => p.IsPrimaryKey()).Select(p => p.GetColumnBaseName()),
            _entityType.GetProperties().Where(p => !p.IsPrimaryKey()).Select(p => p.GetColumnBaseName()));
        return this;
    }

    /// <summary>
    /// Starts a new UPDATE statement for the specified entity type
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to update</typeparam>
    public Context<T, Update> Update<T>()
    {
        NextCommand();
        AppendSql("UPDATE ");
        AppendLiteral(_context.GetEntityType<T>());
        _state = State.Update;
        return new(this);
    }

    /// <summary>
    /// Starts an assignment to the specified column
    /// </summary>
    /// <param name="column">Name of the column to assign</param>
    public SqlBuilder Set(string column)
    {
        if (_state != State.Update && _state != State.Set)
        {
            throw new InvalidOperationException();
        }

        if (_state == State.Update)
        {
            _state = State.Set;
            AppendSql(" SET ");
        }
        else
        {
            AppendSql(",");
        }
        AppendIdentifier(column);
        AppendSql("=");
        return this;
    }

    /// <summary>
    /// Assigns a literal value to the specified column
    /// </summary>
    /// <param name="column">Name of the column to assign</param>
    /// <param name="value">Literal value to assign</param>
    public SqlBuilder Set(string column, object value)
    {
        return Set(column).AppendValue(value);
    }

    /// <summary>
    /// Starts a new DELETE statement for the specified entity type
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity to delete</typeparam>
    public Context<T, Delete> Delete<T>()
    {
        NextCommand();
        AppendSql("DELETE FROM ");
        AppendLiteral(_context.GetEntityType<T>());
        _state = State.Delete;
        return new(this);
    }

    /// <summary>
    /// Begins a nested section of WHERE conditions to be OR-red together
    /// </summary>
    public SqlBuilder WhereOr()
    {
        switch (_state)
        {
            case State.Set:
            case State.Delete:
            case State.From:
            case State.Join:
            case State.JoinOn:
                AppendSql(" WHERE (0");
                break;

            case State.Where:
                AppendSql(" AND (0");
                break;

            default:
                throw new InvalidOperationException();
        }

        _state = State.WhereOr;
        return this;
    }

    /// <summary>
    /// Ends the nested section of WHERE conditions to be OR-red together
    /// </summary>
    public SqlBuilder EndWhereOr()
    {
        if (_state != State.WhereOr)
            throw new InvalidOperationException();

        AppendSql(")");
        _state = State.Where;
        return this;
    }

    private void BeginWhere()
    {
        switch (_state)
        {
            case State.Select:
                From(_entityType ?? throw new InvalidOperationException());
                goto case State.From;

            case State.Set:
            case State.Delete:
            case State.From:
            case State.Join:
            case State.JoinOn:
                _sql.Append(" WHERE ");
                _state = State.Where;
                break;

            case State.Where:
                AppendSql(" AND ");
                break;

            case State.WhereOr:
                AppendSql(" OR ");
                break;

            default:
                throw new InvalidOperationException();
        }
    }

    /// <summary>
    /// Begins the match against a specified column
    /// </summary>
    /// <param name="column">Name of the column to match</param>
    public SqlBuilder Where(string column)
    {
        BeginWhere();
        _sql.Append('(');
        AppendExpression(column);
        _state = State.WhereCondition;
        return this;
    }

    /// <summary>
    /// Appends a subquery as a WHERE condition
    /// </summary>
    /// <param name="builder">A delegate for emitting the subquery</param>
    public SqlBuilder Where(Action<SqlBuilder> builder)
    {
        BeginWhere();
        _sql.Append('(');
        _state = State.Start;
        builder(this);
        _sql.Append(')');
        _state = State.Where;
        return this;
    }

    /// <summary>
    /// Appends a BETWEEN clause to the previously emitted column
    /// </summary>
    /// <typeparam name="T">Type of the values</typeparam>
    /// <param name="left">Lower bound of the BETWEEN clause (inclusive)</param>
    /// <param name="right">Upper bound of the BETWEEN clause (inclusive!)</param>
    public SqlBuilder WhereBetween<T>(T left, T right)
    {
        if (_state != State.WhereCondition)
        {
            throw new InvalidOperationException();
        }

        AppendBetween(left, right);
        _sql.Append(')');
        _state = State.Where;
        return this;
    }

    /// <summary>
    /// Appends any expression or a subquery as the right side of the condition
    /// </summary>
    /// <param name="sql">A delegate for emitting the expression or subquery</param>
    /// <param name="op">Comparison operator to use</param>
    public SqlBuilder Where(Action<SqlBuilder> sql, string op = "=")
    {
        if (_state != State.WhereCondition)
        {
            throw new InvalidOperationException();
        }

        AppendSql(op);
        _sql.Append('(');
        _state = State.Start;
        sql(this);
        _sql.Append("))");
        _state = State.Where;
        return this;
    }

    /// <summary>
    /// Compares the previously specified column to a literal value
    /// </summary>
    /// <param name="value">Literal value to emit for comparison</param>
    /// <param name="op">Comparison operator to use</param>
    public SqlBuilder WhereValue(object? value, string op = "=")
    {
        if (_state != State.WhereCondition)
        {
            throw new InvalidOperationException();
        }

        switch (value)
        {
            case null:
                AppendSql(" IS NULL");
                break;
            case string:
                goto default;
            case IEnumerable e:
                AppendSql(" IN (");
                AppendValues(e);
                AppendSql(")");
                break;
            case FormattableString fs:
                AppendSql(op);
                AppendInterpolated(fs);
                break;
            case SqlBuilder sql:
                AppendSql(" IN (");
                this._sql.Append(sql._sql);
                AppendSql(")");
                break;
            default:
                AppendSql(op);
                AppendValue(value);
                break;
        }
        _sql.Append(')');
        _state = State.Where;

        return this;
    }

    /// <summary>
    /// Appends a calculated expression as a condition in a WHERE clause
    /// </summary>
    /// <param name="condition">A <see cref="FormattableString" /> representing any SQL expression, interpolated values are safely processed into literals</param>
    public SqlBuilder WhereExpression(FormattableString condition)
    {
        BeginWhere();
        _sql.Append('(');
        AppendInterpolated(condition);
        _sql.Append(')');
        return this;
    }

    /// <summary>
    /// Appends a GROUP BY clause to the current statement
    /// </summary>
    /// <param name="columns">Columns on which grouping will be performed</param>
    /// <remark>Can be called multiple times for appending more columns</remark>
    public SqlBuilder GroupBy(params string[] columns)
    {
        AppendSql(" GROUP BY ");
        for (int i = 0; i < columns.Length; i++)
        {
            if (i > 0)
                AppendSql(",");
            AppendExpression(columns[i]);
        }
        return this;
    }

    /// <summary>
    /// Appends a column to the ORDER BY clause of the current statement
    /// </summary>
    /// <param name="column">Column to be added</param>
    /// <param name="descending">Sort direction of the column</param>
    /// <remark>Can be called multiple times for appending multiple columns</remark>
    public SqlBuilder OrderBy(string column, bool descending = false)
    {
        if (_state != State.OrderBy)
        {
            _state = State.OrderBy;
            AppendSql(" ORDER BY ");
        }
        else
        {
            AppendSql(",");
        }
        AppendExpression(column);
        if (descending)
            AppendSql(" DESC");
        return this;
    }

    /// <summary>
    /// Appends a column sorted in descending order to the ORDER BY clause of the current statement
    /// </summary>
    /// <param name="column">Column to be added</param>
    /// <remark>Can be called multiple times for appending multiple columns</remark>
    public SqlBuilder OrderByDescending(string column)
        => OrderBy(column, true);

    /// <summary>
    /// Appends a LIMIT clause to the current statement
    /// </summary>
    /// <param name="count">The maximum number of records to query or process</param>
    public SqlBuilder Limit(int count)
    {
        if (count > 0)
            AppendSql($" LIMIT {count}");
        return this;
    }

    /// <summary>
    /// Appends a LIMIT clause to the current statement
    /// </summary>
    /// <param name="offset">The number of records to skip at the beginning</param>
    /// <param name="count">The maximum number of records to query or process</param>
    public SqlBuilder Limit(int offset, int count)
    {
        if (offset <= 0)
            return Limit(count);
        if (count <= 0)
            count = int.MaxValue;
        AppendSql($" LIMIT {offset}, {count}");
        return this;
    }

    /// <summary>
    /// Removes the last started command; can be useful for flows where e.g. the number
    /// of rows to INSERT is not known ahead of time
    /// </summary>
    public bool RemoveEmptyCommand()
    {
        switch (_state)
        {
            case State.Select:
            case State.Insert:
            case State.Update:
            case State.Delete:
                _sql.Length = _lastStart;
                _state = State.Start;
                return true;

            default:
                return false;
        }
    }

    #endregion

    #region Low-level helpers

    /// <summary>
    /// Low-level helper; appends the literal SQL, correctly escaping interpolated values as literals
    /// </summary>
    /// <param name="sql">A <see cref="FormattableString" /> representing any SQL, interpolated values are safely processed into literals</param>
    public SqlBuilder AppendInterpolated(FormattableString sql)
    {
        object Literal(object? value) => CloneEmpty().AppendLiteral(value);

        switch (sql.ArgumentCount)
        {
            case 0: return AppendSql(sql.Format);
            case 1: _sql.AppendFormat(_invar, sql.Format, Literal(sql.GetArgument(0))); break;
            case 2: _sql.AppendFormat(_invar, sql.Format, Literal(sql.GetArgument(0)), Literal(sql.GetArgument(1))); break;
            case 3: _sql.AppendFormat(_invar, sql.Format, Literal(sql.GetArgument(0)), Literal(sql.GetArgument(1)), Literal(sql.GetArgument(2))); break;
            default: _sql.AppendFormat(_invar, sql.Format, Array.ConvertAll(sql.GetArguments(), Literal)); break;
        }

        return this;
    }

    /// <summary>
    /// Low-level helper; appends any SQL verbatim
    /// </summary>
    /// <param name="sql">The SQL to append</param>
    public SqlBuilder AppendSql(string? sql)
    {
        this._sql.Append(sql);
        return this;
    }

    /// <summary>
    /// Low-level helper; appends an identifier, quoting it as necessary
    /// </summary>
    /// <param name="name">Identifier to append</param>
    public SqlBuilder AppendIdentifier(string name)
    {
        _syntax.QuoteIdentifier(_sql, name);
        return this;
    }

    /// <summary>
    /// Low-level helper; appends an expression, quoting all identifiers in it as necessary
    /// </summary>
    /// <param name="expression">The expression to append</param>
    /// <remark>
    /// Note that only identifiers found in the expression are quoted.
    /// Literals need to be emitted separately using e.g. <see cref="AppendValue(object?)" />
    /// </remark>
    public SqlBuilder AppendExpression(string expression)
    {
        // quote every identifier not followed by (
        int proc = 0;
        int id = -1;
        for (int i = 0; i < expression.Length; i++)
        {
            if (id == -1)
            {
                // looking for start of identifier
                if (char.IsLetter(expression, i))
                {
                    id = i;
                    if (proc < id)
                    {
                        _sql.Append(expression, proc, id - proc);
                        proc = id;
                    }
                }
            }
            else
            {
                // looking for the end of identifier
                if (!char.IsLetterOrDigit(expression, i))
                {
                    if (expression[i] == '(')
                    {
                        // identifier was apparently function name, leave it as is
                        id = -1;
                    }
                    else
                    {
                        // escape identifier
                        _syntax.QuoteIdentifier(_sql, expression.AsSpan(id, i - id));
                        id = -1;
                        proc = i;
                    }
                }
            }
        }

        if (proc < expression.Length)
        {
            if (id != -1)
            {
                _syntax.QuoteIdentifier(_sql, expression.AsSpan(proc));
            }
            else
            {
                _sql.Append(expression.AsSpan(proc));
            }
        }

        return this;
    }

    /// <summary>
    /// Low-level helper; appends a single literal value
    /// </summary>
    /// <param name="value">The literal value to append</param>
    public SqlBuilder AppendValue(object? value) => AppendLiteral(value);

    /// <summary>
    /// Low-level helper; appends a subquery as a parenthesized value
    /// </summary>
    /// <param name="subQuery">A delegate to emit the subquery</param>
    public SqlBuilder AppendValue(Action<SqlBuilder> subQuery)
    {
        AppendSql("(");
        var prevState = _state;
        _state = State.Start;
        subQuery(this);
        AppendSql(")");
        _state = prevState;
        return this;
    }

    /// <summary>
    /// Low-level helper; appends all the column names from the mapped entity as a comma-separated list
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    public SqlBuilder AppendColumns<T>() => AppendColumns(_context.GetEntityType<T>());
    private SqlBuilder AppendColumns(IEntityType type)
    {
        string? separator = null;
        foreach (var prop in type.GetProperties())
        {
            AppendSql(separator).AppendIdentifier(prop.GetColumnBaseName());
            separator = ",";
        }
        return this;
    }

    /// <summary>
    /// Low-level helper; appends all column values from the mapped entity as a parenthesized, comma-separated list for an INSERT statement
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="row">Actual entity whose values will be emitted</param>
    public SqlBuilder AppendInsertValues<T>(T row) where T : notnull
    {
        return AppendSql(" (").AppendValues(_context.GetEntityType<T>(), row).AppendSql(")");
    }

    /// <summary>
    /// Low-level helper; appends all column values from the mapped entity as a comma-separated list
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity</typeparam>
    /// <param name="row">Actual entity whose values will be emitted</param>
    public SqlBuilder AppendValues<T>(T row) where T : notnull
    {
        return AppendValues(_context.GetEntityType<T>(), row);
    }

    private SqlBuilder AppendValues(IEntityType entityType, object row)
    {
        string? separator = null;
        foreach (var prop in entityType.GetProperties())
        {
            var value = prop.PropertyInfo?.GetValue(row);
            AppendSql(separator);
            if (prop.ValueGenerated == ValueGenerated.OnAdd && ((value is int i && i == 0) || (value is long l && l == 0)))
                value = null;
            if (value != null && prop.GetValueConverter() is ValueConverter converter)
                value = converter.ConvertToProvider(value);
            AppendValue(value);
            separator = ",";
        }
        return this;
    }

    private SqlBuilder AppendValues(IEnumerable values)
    {
        string? separator = null;
        foreach (var value in values)
        {
            AppendSql(separator).AppendValue(value);
            separator = ",";
        }
        return this;
    }

    private SqlBuilder AppendBetween(object? left, object? right)
    {
        AppendSql(" BETWEEN ");
        AppendValue(left);
        AppendSql(" AND ");
        AppendValue(right);
        return this;
    }

    #endregion

    /// <summary>
    /// Returns the SQL generated so far
    /// </summary>
    public override string ToString()
    {
        return _sql == null ? "" : _sql.ToString();
    }

    /// <summary>
    /// Clones the <see cref="SqlBuilder" /> including its state, effectively creating a "fork"
    /// </summary>
    public SqlBuilder Clone()
    {
        var res = (SqlBuilder)MemberwiseClone();
        res._sql = new StringBuilder();
        res._sql.Append(_sql);
        return res;
    }

    /// <summary>
    /// Creates a new, empty <see cref="SqlBuilder" /> using the same <see cref="DbContext" /> and <see cref="SqlSyntax" />
    /// </summary>
    private SqlBuilder CloneEmpty()
        => new(_context, _syntax);

    /// <summary>
    /// Creates a new <see cref="SqlBuilder" />, replacing the existing SELECT columns with a COUNT(*)
    /// </summary>
    public SqlBuilder ToCount()
    {
        var res = new SqlBuilder(_context, _syntax)
        {
            _lastStart = _lastStart,
            _state = _state
        };

        if (_columnsEnd != 0)
        {
            res._sql.Append(_sql, 0, _columnsStart);
            res._columnsStart = _sql.Length;
            res._sql.Append("COUNT(*)");
            res._columnsEnd = _sql.Length;
            res._sql.Append(_sql, _columnsEnd, _sql.Length - _columnsEnd);
        }

        return res;
    }

    /// <summary>
    /// Creates a new <see cref="SqlBuilder" />, replacing the existing SELECT statement with a DELETE
    /// </summary>
    public SqlBuilder ToDelete()
    {
        if (_columnsEnd == 0)
            throw new InvalidOperationException();

        var res = new SqlBuilder(_context, _syntax)
        {
            _state = _state
        };
        res._sql.Append("DELETE");
        res._sql.Append(_sql, _columnsEnd, _sql.Length - _columnsEnd);
        return res;
    }

    private SqlBuilder AppendLiteral(object? value)
    {
        if (value == null)
        {
            return AppendSql("NULL");
        }
        if (value is IConvertible conv)
        {
            return AppendLiteral(conv);
        }
        if (value is byte[] binary)
        {
            return AppendLiteral(binary);
        }
        if (FindEntityType(value) is { } entityType)
        {
            return AppendIdentifier(entityType.GetTableName() ?? throw new ArgumentException("Entity not mapped to a table"));
        }
        return AppendLiteral(value.ToString());
    }

    static IEntityType? FindEntityType(object value)
    {
        if (value is IEntityType et)
        {
            return et;
        }

        var type = value.GetType();
        if (type.GetGenericTypeDefinition().IsSubclassOf(typeof(DbSet<>)))
            return null;

        var db = (value as IInfrastructure<IServiceProvider>)?.GetService<ICurrentDbContext>()?.Context;
        if (db == null)
            return null;

        return db.Model.FindEntityType(type.GetGenericArguments()[0]);
    }

    private SqlBuilder AppendLiteral(IConvertible value)
    {
        switch (value.GetTypeCode())
        {
            case TypeCode.Boolean:
                return AppendSql(value.ToBoolean(_invar) ? "1" : "0");

            case TypeCode.Byte:
            case TypeCode.Decimal:
            case TypeCode.Double:
            case TypeCode.Int16:
            case TypeCode.Int32:
            case TypeCode.Int64:
            case TypeCode.SByte:
            case TypeCode.Single:
            case TypeCode.UInt16:
            case TypeCode.UInt32:
            case TypeCode.UInt64:
                // all integral literals are simply converted using the invariant culture
                return AppendSql(value.ToString(_invar));

            case TypeCode.Char:
            case TypeCode.String:
                return AppendLiteral(value.ToString());

            case TypeCode.DateTime:
                return AppendLiteral(value.ToDateTime(_invar).ToString("yyyy-MM-dd HH:mm:ss", _invar));

            case TypeCode.DBNull:
                return AppendSql("NULL");

            default:
                throw new ArgumentException($"value of type {value.GetType()} cannot be converted to an SQL literal", "value");
        }
    }

    private SqlBuilder AppendLiteral(string? value)
    {
        if (value == null)
        {
            _sql.Append("NULL");
        }
        else
        {
            _syntax.StringLiteral(_sql, value);
        }
        return this;
    }

    private SqlBuilder AppendLiteral(byte[] binary)
    {
        _syntax.BinaryLiteral(_sql, binary.AsSpan());
        return this;
    }

    #region Execution helper
    internal delegate void AddToResultDelegate<TResult>(IDataRecord record, ref TResult result);

    [Flags]
    internal enum ExecuteOptions
    {
        None = 0,
        NoDefault = 1,
        Single = 2,
    }

    internal async Task<T> ExecuteAsync<T>(T res, Func<DbDataReader, AddToResultDelegate<T>?> getResultProcessor, ExecuteOptions options = ExecuteOptions.None)
    {
        bool any = false;

        if (_sql.Length > 0)
        {
            var db = _context.Database;
            var cb = db.GetService<IRawSqlCommandBuilder>();
            var con = db.GetService<IRelationalConnection>();
#if NET5_0
            var logger = db.GetService<IDiagnosticsLogger<DbLoggerCategory.Database.Command>>();
#else
            var logger = db.GetService<IRelationalCommandDiagnosticsLogger>();
#endif
            var cmd = cb.Build(_sql.ToString());
            using var cmdDisposable = cmd as IDisposable;
            await using var rdr = await cmd.ExecuteReaderAsync(new RelationalCommandParameterObject(con, null, null, _context, logger));
            var dbRdr = rdr.DbDataReader;

            var resultProcessor = getResultProcessor(dbRdr);
            for (; ; )
            {
                while (await rdr.ReadAsync())
                {
                    if (any && (options & ExecuteOptions.Single) != 0)
                    {
                        throw new InvalidOperationException("More than one matching record found");
                    }
                    resultProcessor?.Invoke(dbRdr, ref res);
                }

                if (!await dbRdr.NextResultAsync())
                {
                    break;
                }

                resultProcessor = getResultProcessor(dbRdr);
            }
        }

        if (!any && (options & ExecuteOptions.NoDefault) != 0)
        {
            throw new InvalidOperationException("No matching record found");
        }

        return res;
    }
    #endregion
}
