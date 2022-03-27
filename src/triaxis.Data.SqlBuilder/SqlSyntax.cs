using Microsoft.EntityFrameworkCore;

namespace triaxis.Data.SqlBuilder;

/// <summary>
/// Provides the details of various SQL dialects
/// </summary>
public abstract partial class SqlSyntax
{
    /// <summary>
    /// Gets the MySql/MariaDB <see cref="SqlSyntax" />
    /// </summary>
    public static readonly SqlSyntax MySql = new MySqlSyntax();
    /// <summary>
    /// Gets the Sqlite <see cref="SqlSyntax" />
    /// </summary>
    public static readonly SqlSyntax Sqlite = new SqliteSyntax();

    /// <summary>
    /// Get the string used to separate individual commands
    /// </summary>
    public virtual string CommandSeparator => "\n;\n";

    /// <summary>
    /// Get the statement for retrieval of last INSERT-ed identity value
    /// </summary>
    public abstract string LastIdentityExpression { get; }

    /// <summary>
    /// Emits a safely quoted identifier
    /// </summary>
    /// <param name="output">The <see cref="StringBuilder" /> to which the identifier will be appended</param>
    /// <param name="identifier">The identifier to emit</param>
    public abstract void QuoteIdentifier(StringBuilder output, ReadOnlySpan<char> identifier);

    /// <summary>
    /// Emits a literal string value
    /// </summary>
    /// <param name="output">The <see cref="StringBuilder" /> to which the string literal will be appended</param>
    /// <param name="value">The string value to emit</param>
    public virtual void StringLiteral(StringBuilder output, ReadOnlySpan<char> value)
        => output.Append('\'').AppendDoubleEscaped(value, "\\\'").Append('\'');

    /// <summary>
    /// Emits a literal binary value
    /// </summary>
    /// <param name="output">The <see cref="StringBuilder" /> to which the binary literal will be appended</param>
    /// <param name="value">The span of bytes to emit</param>
    public abstract void BinaryLiteral(StringBuilder output, ReadOnlySpan<byte> value);

    /// <summary>
    /// Appends the ON DUPLICATE KEY UPDATE clause to an insert statement, turning it into an UPSERT
    /// </summary>
    /// <param name="output">The <see cref="StringBuilder" /> to which the clause will be appended</param>
    /// <param name="matchColumns">Set of columns for matching duplicates</param>
    /// <param name="updateColumns">Set of columns to be updated in case a duplicate is found</param>
    public abstract void OnDuplicateKeyUpdate(StringBuilder output, IEnumerable<string> matchColumns, IEnumerable<string> updateColumns);

    /// <summary>
    /// Attempts to retrieve the correct SQL dialect for the provided <see cref="DbContext" />
    /// </summary>
    /// <param name="context">The <see cref="DbContext" /> for which SQL dialect is required</param>
    public static SqlSyntax FromContext(DbContext context)
    {
        if (context.Database.ProviderName is { } providerName)
        {
            if (providerName.Contains("mysql", StringComparison.OrdinalIgnoreCase))
            {
                return MySql;
            }
            if (providerName.Contains("sqlite", StringComparison.OrdinalIgnoreCase))
            {
                return Sqlite;
            }
        }

        throw new ArgumentException("Unable to determine SQL syntax from context provider", nameof(context));
    }
}
