namespace triaxis.Data.SqlBuilder;

partial class SqlSyntax
{
    class SqliteSyntax : SqlSyntax
    {
        public override string LastIdentityExpression => "LAST_INSERT_ROWID()";

        public override void QuoteIdentifier(StringBuilder output, ReadOnlySpan<char> identifier)
            => output.Append('[').Append(identifier).Append(']');

        public override void BinaryLiteral(StringBuilder output, ReadOnlySpan<byte> value)
            => output.Append("x'").AppendHex(value).Append('\'');

        public override void OnDuplicateKeyUpdate(StringBuilder output, IEnumerable<string> matchColumns, IEnumerable<string> updateColumns)
        {
            output.Append(" ON CONFLICT");
            char separator = '(';
            foreach (var col in matchColumns)
            {
                output.Append(separator);
                QuoteIdentifier(output, col);
                separator = ',';
            }
            output.Append(") DO UPDATE SET");
            separator = ' ';
            foreach (var col in updateColumns)
            {
                output.Append(separator);
                QuoteIdentifier(output, col);
                output.Append("=EXCLUDED.");
                QuoteIdentifier(output, col);
                output.Append(')');
                separator = ',';
            }
        }
    }
}
