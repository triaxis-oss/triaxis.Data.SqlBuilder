namespace triaxis.Data.SqlBuilder;

partial class SqlSyntax
{
    class MySqlSyntax : SqlSyntax
    {
        public override string LastIdentityExpression => "LAST_INSERT_ID()";

        public override void QuoteIdentifier(StringBuilder output, ReadOnlySpan<char> identifier)
            => output.Append('`').Append(identifier).Append('`');

        public override void BinaryLiteral(StringBuilder output, ReadOnlySpan<byte> value)
            => output.Append("x'").AppendHex(value).Append('\'');

        public override void OnDuplicateKeyUpdate(StringBuilder output, IEnumerable<string> matchColumns, IEnumerable<string> updateColumns)
        {
            output.Append(" ON DUPLICATE KEY UPDATE");

            char? separator = ' ';
            foreach (var col in updateColumns)
            {
                output.Append(separator);
                QuoteIdentifier(output, col);
                output.Append("=VALUES(");
                QuoteIdentifier(output, col);
                output.Append(')');
                separator = ',';
            }
        }
    }
}
