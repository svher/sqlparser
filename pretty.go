package sqlparser

import "strings"

// PrettyFormatter is a NodeFormatter implementation that produces a
// human-friendly, multi-line rendering of SQL nodes when pretty output is
// requested. It falls back to the default formatting for nodes that don't
// require special handling.
func PrettyFormatter(buf *TrackedBuffer, node SQLNode) {
	if buf == nil || node == nil {
		return
	}

	switch node := node.(type) {
	case *Select:
		node.formatPretty(buf)
	case *Where:
		prettyFormatWhere(buf, node)
	case GroupBy:
		prettyFormatGroupBy(buf, node)
	case OrderBy:
		prettyFormatOrderBy(buf, node)
	case *Limit:
		prettyFormatLimit(buf, node)
	default:
		node.Format(buf, true)
	}
}

func (node *Select) formatPretty(buf *TrackedBuffer) {
	var b strings.Builder

	if len(node.Comments) > 0 {
		comments := strings.TrimSpace(String(node.Comments, false))
		if comments != "" {
			b.WriteString(comments)
			b.WriteByte('\n')
		}
	}

	b.WriteString("select")

	selectPrefixLen := len("select")

	appendClause := func(value string) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return
		}
		b.WriteByte(' ')
		b.WriteString(trimmed)
		selectPrefixLen += 1 + len(trimmed)
	}

	appendClause(node.Cache)
	appendClause(node.Distinct)
	appendClause(node.Hints)

	if len(node.SelectExprs) > 0 {
		indent := strings.Repeat(" ", selectPrefixLen+1)
		b.WriteByte(' ')
		for i, expr := range node.SelectExprs {
			if i > 0 {
				b.WriteString(",\n")
				b.WriteString(indent)
			}
			b.WriteString(String(expr, false))
		}
	}

	if len(node.From) > 0 {
		b.WriteString("\nfrom ")
		for i, table := range node.From {
			if i > 0 {
				b.WriteString(", ")
			}
			if pretty, ok := prettyTableExpr(table); ok {
				b.WriteString(pretty)
				continue
			}
			b.WriteString(String(table, false))
		}
	}

	if node.Where != nil && node.Where.Expr != nil {
		b.WriteByte('\n')
		b.WriteString(node.Where.Type)
		b.WriteByte(' ')
		b.WriteString(String(node.Where.Expr, false))
	}

	if len(node.GroupBy) > 0 {
		b.WriteString("\ngroup by ")
		for i, expr := range node.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(String(expr, false))
		}
	}

	if node.Having != nil && node.Having.Expr != nil {
		b.WriteByte('\n')
		b.WriteString(node.Having.Type)
		b.WriteByte(' ')
		b.WriteString(String(node.Having.Expr, false))
	}

	if len(node.OrderBy) > 0 {
		b.WriteString("\norder by ")
		for i, order := range node.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(String(order, false))
		}
	}

	if node.Limit != nil {
		b.WriteString("\nlimit ")
		if node.Limit.Offset != nil {
			b.WriteString(String(node.Limit.Offset, false))
			b.WriteString(", ")
		}
		b.WriteString(String(node.Limit.Rowcount, false))
	}

	if node.Lock != "" {
		lock := strings.TrimSpace(node.Lock)
		if lock != "" {
			b.WriteByte('\n')
			b.WriteString(lock)
		}
	}

	buf.WriteString(b.String())
}

func prettyFormatWhere(buf *TrackedBuffer, node *Where) {
	if node == nil || node.Expr == nil {
		return
	}
	buf.Myprintf("\n%s %v", node.Type, node.Expr)
}

func prettyFormatGroupBy(buf *TrackedBuffer, node GroupBy) {
	if len(node) == 0 {
		return
	}
	buf.WriteString("\ngroup by ")
	for i, expr := range node {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.Myprintf("%v", expr)
	}
}

func prettyFormatOrderBy(buf *TrackedBuffer, node OrderBy) {
	if len(node) == 0 {
		return
	}
	buf.WriteString("\norder by ")
	for i, expr := range node {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.Myprintf("%v", expr)
	}
}

func prettyFormatLimit(buf *TrackedBuffer, node *Limit) {
	if node == nil {
		return
	}
	buf.WriteString("\nlimit ")
	if node.Offset != nil {
		buf.Myprintf("%v, ", node.Offset)
	}
	buf.Myprintf("%v", node.Rowcount)
}

func prettyTableExpr(expr TableExpr) (string, bool) {
	switch t := expr.(type) {
	case *AliasedTableExpr:
		switch sub := t.Expr.(type) {
		case *Subquery:
			inner := String(sub.Select, true)
			var sb strings.Builder
			sb.WriteString("(\n")
			sb.WriteString(indentLines(inner, "\t"))
			sb.WriteString("\n)")
			if len(t.Partitions) > 0 {
				sb.WriteString(String(t.Partitions, false))
			}
			if !t.As.IsEmpty() {
				sb.WriteString(" as ")
				sb.WriteString(t.As.String())
			}
			if t.Hints != nil {
				sb.WriteString(String(t.Hints, false))
			}
			return sb.String(), true
		}
	}
	return "", false
}

func indentLines(s, indent string) string {
	if s == "" {
		return s
	}
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = indent + line
	}
	return strings.Join(lines, "\n")
}
