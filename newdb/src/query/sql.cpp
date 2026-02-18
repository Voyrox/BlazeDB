#include "query/sql.h"

#include "query/schema.h"

#include <cctype>

using std::string;

namespace blazeDb
{

static bool isIdentChar(char c)
{
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

static void skipWhitespace(stringView s, usize& i)
{
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i])))
        i++;
}

static bool matchKeyword(stringView s, usize& i, stringView keyChars)
{
    skipWhitespace(s, i);
    usize j = i;
    for (char c : keyChars)
    {
        if (j >= s.size())
            return false;
        char tmp = s[j];
        if (std::tolower(static_cast<unsigned char>(tmp)) != std::tolower(static_cast<unsigned char>(c)))
            return false;
        j++;
    }
    if (j < s.size() && isIdentChar(s[j]))
        return false;
    i = j;
    return true;
}

static bool consumeChar(stringView s, usize& i, char c)
{
    skipWhitespace(s, i);
    if (i < s.size() && s[i] == c)
    {
        i++;
        return true;
    }
    return false;
}

static bool parseIdentifier(stringView s, usize& i, string& out)
{
    skipWhitespace(s, i);
    if (i >= s.size() || !isIdentChar(s[i]) || std::isdigit(static_cast<unsigned char>(s[i])))
        return false;
    usize start = i;
    while (i < s.size() && isIdentChar(s[i]))
        i++;
    out.assign(s.substr(start, i - start));
    return true;
}

static bool parseQuoted(stringView s, usize& i, string& out)
{
    skipWhitespace(s, i);
    if (i >= s.size() || s[i] != '"')
        return false;
    i++;
    string result;
    while (i < s.size())
    {
        char c = s[i++];
        if (c == '"')
        {
            out = result;
            return true;
        }
        if (c == '\\' && i < s.size())
        {
            char nextc = s[i++];
            if (nextc == '"' || nextc == '\\' || nextc == '/')
                result.push_back(nextc);
            else if (nextc == 'n')
                result.push_back('\n');
            else if (nextc == 'r')
                result.push_back('\r');
            else if (nextc == 't')
                result.push_back('\t');
            else
                result.push_back(nextc);
        }
        else
        {
            result.push_back(c);
        }
    }
    return false;
}

static string stripTrailingSemicolon(string s)
{
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))
        s.pop_back();
    if (!s.empty() && s.back() == ';')
        s.pop_back();
    return s;
}

static bool parseIfNotExists(stringView s, usize& i, bool& out)
{
    out = false;
    usize j = i;
    if (!matchKeyword(s, j, "if"))
        return true;
    if (!matchKeyword(s, j, "not"))
        return false;
    if (!matchKeyword(s, j, "exists"))
        return false;
    i = j;
    out = true;
    return true;
}

static bool parseHexLiteral(stringView s, usize& i, string& out)
{
    skipWhitespace(s, i);
    if (i + 2 > s.size())
        return false;
    if (s[i] != '0' || (s[i + 1] != 'x' && s[i + 1] != 'X'))
        return false;
    usize j = i + 2;
    while (j < s.size())
    {
        char c = s[j];
        bool ok = std::isdigit(static_cast<unsigned char>(c)) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        if (!ok)
            break;
        j++;
    }
    if (j == i + 2)
        return false;
    out.assign(s.substr(i + 2, j - (i + 2)));
    i = j;
    return true;
}

static bool parseNumberToken(stringView s, usize& i, string& out)
{
    skipWhitespace(s, i);
    usize j = i;
    if (j < s.size() && (s[j] == '-' || s[j] == '+'))
        j++;
    bool any = false;
    while (j < s.size() && std::isdigit(static_cast<unsigned char>(s[j])))
    {
        any = true;
        j++;
    }
    if (j < s.size() && s[j] == '.')
    {
        j++;
        while (j < s.size() && std::isdigit(static_cast<unsigned char>(s[j])))
        {
            any = true;
            j++;
        }
    }
    if (!any)
        return false;
    out.assign(s.substr(i, j - i));
    i = j;
    return true;
}

static bool parseLiteral(stringView s, usize& i, SqlLiteral& out)
{
    skipWhitespace(s, i);
    string tmp;
    usize j = i;

    if (matchKeyword(s, j, "null"))
    {
        i = j;
        out.kind = SqlLiteral::Kind::Null;
        out.text.clear();
        return true;
    }

    j = i;
    if (matchKeyword(s, j, "true"))
    {
        i = j;
        out.kind = SqlLiteral::Kind::Bool;
        out.text = "true";
        return true;
    }
    j = i;
    if (matchKeyword(s, j, "false"))
    {
        i = j;
        out.kind = SqlLiteral::Kind::Bool;
        out.text = "false";
        return true;
    }

    j = i;
    if (matchKeyword(s, j, "b64"))
    {
        i = j;
        if (!parseQuoted(s, i, tmp))
            return false;
        out.kind = SqlLiteral::Kind::Base64;
        out.text = tmp;
        return true;
    }

    if (parseHexLiteral(s, i, tmp))
    {
        out.kind = SqlLiteral::Kind::Hex;
        out.text = tmp;
        return true;
    }

    if (parseQuoted(s, i, tmp))
    {
        out.kind = SqlLiteral::Kind::Quoted;
        out.text = tmp;
        return true;
    }

    if (parseNumberToken(s, i, tmp))
    {
        out.kind = SqlLiteral::Kind::Number;
        out.text = tmp;
        return true;
    }

    return false;
}

static bool parseTypeName(stringView s, usize& i, string& out)
{
    string type;
    if (!parseIdentifier(s, i, type))
        return false;
    usize j = i;
    string size;
    if (type == "int" && parseNumberToken(s, j, size))
    {
        if (size == "32" || size == "64")
        {
            i = j;
            out = "int" + size;
            return true;
        }
    }
    out = type;
    return true;
}

std::optional<SqlCommand> parseSqlLine(const string& rawLine, string& error)
{
    error.clear();
    string line = stripTrailingSemicolon(rawLine);
    stringView s(line);
    usize i = 0;

    skipWhitespace(s, i);
    if (i >= s.size())
    {
        error = "empty";
        return std::nullopt;
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "ping"))
            return SqlPing{};
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "create"))
        {
            i = j;
            if (matchKeyword(s, i, "keyspace"))
            {
                SqlCreateKeyspace cmd;
                if (!parseIfNotExists(s, i, cmd.ifNotExists))
                {
                    error = "Expected not exists";
                    return std::nullopt;
                }
                if (!parseIdentifier(s, i, cmd.keyspace))
                {
                    error = "Expected keyspace";
                    return std::nullopt;
                }
                return cmd;
            }
            if (matchKeyword(s, i, "table"))
            {
                SqlCreateTable cmd;
                if (!parseIfNotExists(s, i, cmd.ifNotExists))
                {
                    error = "Expected not exists";
                    return std::nullopt;
                }
                if (!parseIdentifier(s, i, cmd.keyspace))
                {
                    error = "Expected keyspace";
                    return std::nullopt;
                }
                if (!consumeChar(s, i, '.'))
                {
                    error = "Expected dot";
                    return std::nullopt;
                }
                if (!parseIdentifier(s, i, cmd.table))
                {
                    error = "Expected table";
                    return std::nullopt;
                }
                if (!consumeChar(s, i, '('))
                {
                    error = "Expected (";
                    return std::nullopt;
                }

                std::vector<ColumnDef> cols;
                string pkName;
                while (true)
                {
                    skipWhitespace(s, i);
                    if (consumeChar(s, i, ')'))
                        break;

                    usize k = i;
                    if (matchKeyword(s, k, "primary"))
                    {
                        i = k;
                        if (!matchKeyword(s, i, "key"))
                        {
                            error = "Expected key";
                            return std::nullopt;
                        }
                        if (!consumeChar(s, i, '('))
                        {
                            error = "Expected (";
                            return std::nullopt;
                        }
                        if (!parseIdentifier(s, i, pkName))
                        {
                            error = "Expected pk";
                            return std::nullopt;
                        }
                        if (!consumeChar(s, i, ')'))
                        {
                            error = "Expected )";
                            return std::nullopt;
                        }
                    }
                    else
                    {
                        string colName;
                        string typeName;
                        if (!parseIdentifier(s, i, colName))
                        {
                            error = "Expected column";
                            return std::nullopt;
                        }
                        if (!parseTypeName(s, i, typeName))
                        {
                            error = "Expected type";
                            return std::nullopt;
                        }
                        auto ct = columnTypeFromName(typeName);
                        if (!ct.has_value())
                        {
                            error = "unknown type";
                            return std::nullopt;
                        }
                        cols.push_back(ColumnDef{colName, *ct});
                    }

                    skipWhitespace(s, i);
                    if (consumeChar(s, i, ','))
                        continue;
                    if (consumeChar(s, i, ')'))
                        break;
                    error = "Expected , or )";
                    return std::nullopt;
                }

                if (pkName.empty())
                {
                    error = "missing primary key";
                    return std::nullopt;
                }

                TableSchema schema;
                schema.columns = std::move(cols);
                auto pkIdx = findColumnIndex(schema, pkName);
                if (!pkIdx.has_value())
                {
                    error = "pk not in columns";
                    return std::nullopt;
                }
                schema.primaryKeyIndex = *pkIdx;
                cmd.schema = std::move(schema);
                return cmd;
            }
            error = "Expected keyspace or table";
            return std::nullopt;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "insert"))
        {
            i = j;
            if (!matchKeyword(s, i, "into"))
            {
                error = "Expected into";
                return std::nullopt;
            }
            SqlInsert cmd;
            if (!parseIdentifier(s, i, cmd.keyspace))
            {
                error = "Expected keyspace";
                return std::nullopt;
            }
            if (!consumeChar(s, i, '.'))
            {
                error = "Expected dot";
                return std::nullopt;
            }
            if (!parseIdentifier(s, i, cmd.table))
            {
                error = "Expected table";
                return std::nullopt;
            }
            if (!consumeChar(s, i, '('))
            {
                error = "Expected (";
                return std::nullopt;
            }
            while (true)
            {
                string col;
                if (!parseIdentifier(s, i, col))
                {
                    error = "Expected column";
                    return std::nullopt;
                }
                cmd.columns.push_back(col);
                if (consumeChar(s, i, ','))
                    continue;
                if (consumeChar(s, i, ')'))
                    break;
                error = "Expected , or )";
                return std::nullopt;
            }
            if (!matchKeyword(s, i, "values"))
            {
                error = "Expected values";
                return std::nullopt;
            }

            while (true)
            {
                if (!consumeChar(s, i, '('))
                {
                    error = "Expected (";
                    return std::nullopt;
                }

                vector<SqlLiteral> row;
                while (true)
                {
                    SqlLiteral lit;
                    if (!parseLiteral(s, i, lit))
                    {
                        error = "Expected literal";
                        return std::nullopt;
                    }
                    row.push_back(lit);
                    if (consumeChar(s, i, ','))
                        continue;
                    if (consumeChar(s, i, ')'))
                        break;
                    error = "Expected , or )";
                    return std::nullopt;
                }

                if (row.size() != cmd.columns.size())
                {
                    error = "Wrong values count";
                    return std::nullopt;
                }
                cmd.rows.push_back(std::move(row));

                skipWhitespace(s, i);
                if (consumeChar(s, i, ','))
                    continue;
                break;
            }

            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "delete"))
        {
            i = j;
            if (!matchKeyword(s, i, "from"))
            {
                error = "Expected from";
                return std::nullopt;
            }
            SqlDelete cmd;
            if (!parseIdentifier(s, i, cmd.keyspace))
            {
                error = "Expected keyspace";
                return std::nullopt;
            }
            if (!consumeChar(s, i, '.'))
            {
                error = "Expected dot";
                return std::nullopt;
            }
            if (!parseIdentifier(s, i, cmd.table))
            {
                error = "Expected table";
                return std::nullopt;
            }
            if (!matchKeyword(s, i, "where"))
            {
                error = "Expected where";
                return std::nullopt;
            }
            if (!parseIdentifier(s, i, cmd.whereColumn))
            {
                error = "Expected where column";
                return std::nullopt;
            }
            if (!consumeChar(s, i, '='))
            {
                error = "Expected =";
                return std::nullopt;
            }
            if (!parseLiteral(s, i, cmd.whereValue))
            {
                error = "Expected literal";
                return std::nullopt;
            }
            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "select"))
        {
            i = j;
            SqlSelect cmd;
            if (consumeChar(s, i, '*'))
            {
            }
            else
            {
                while (true)
                {
                    string col;
                    if (!parseIdentifier(s, i, col))
                    {
                        error = "Expected column";
                        return std::nullopt;
                    }
                    cmd.columns.push_back(col);
                    if (consumeChar(s, i, ','))
                        continue;
                    break;
                }
            }
            if (!matchKeyword(s, i, "from"))
            {
                error = "Expected from";
                return std::nullopt;
            }
            if (!parseIdentifier(s, i, cmd.keyspace))
            {
                error = "Expected keyspace";
                return std::nullopt;
            }
            if (!consumeChar(s, i, '.'))
            {
                error = "Expected dot";
                return std::nullopt;
            }
            if (!parseIdentifier(s, i, cmd.table))
            {
                error = "Expected table";
                return std::nullopt;
            }
            if (!matchKeyword(s, i, "where"))
            {
                error = "Expected where";
                return std::nullopt;
            }
            if (!parseIdentifier(s, i, cmd.whereColumn))
            {
                error = "Expected where column";
                return std::nullopt;
            }
            if (!consumeChar(s, i, '='))
            {
                error = "Expected =";
                return std::nullopt;
            }
            if (!parseLiteral(s, i, cmd.whereValue))
            {
                error = "Expected literal";
                return std::nullopt;
            }
            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "flush"))
        {
            i = j;
            SqlFlush cmd;
            if (!parseIdentifier(s, i, cmd.keyspace))
            {
                error = "Expected keyspace";
                return std::nullopt;
            }
            if (!consumeChar(s, i, '.'))
            {
                error = "Expected dot";
                return std::nullopt;
            }
            if (!parseIdentifier(s, i, cmd.table))
            {
                error = "Expected table";
                return std::nullopt;
            }
            return cmd;
        }
    }

    error = "unknown";
    return std::nullopt;
}

}
