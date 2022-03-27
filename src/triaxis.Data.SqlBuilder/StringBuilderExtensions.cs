namespace triaxis.Data.SqlBuilder;

internal static class StringBuilderExtensions
{
    private static readonly string _hex = "0123456789ABCDEF";

    public static StringBuilder AppendHex(this StringBuilder builder, ReadOnlySpan<byte> bytes)
    {
        foreach (var b in bytes)
        {
            builder.Append(_hex[b >> 4]);
            builder.Append(_hex[b & 0xF]);
        }
        return builder;
    }

    public static StringBuilder AppendDoubleEscaped(this StringBuilder builder, ReadOnlySpan<char> chars, ReadOnlySpan<char> escapeChars)
    {
        while (chars.Length > 0)
        {
            int i = chars.IndexOfAny(escapeChars);
            if (i == -1)
            {
                builder.Append(chars);
                break;
            }
            builder.Append(chars[..(i + 1)]);
            builder.Append(chars[i]);
            chars = chars[(i + 1)..];
        }
        return builder;
    }
}
