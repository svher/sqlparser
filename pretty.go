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
		prettyFormatSelect(buf, node)
	case *Where:
		prettyFormatWhere(buf, node)
	case GroupBy:
		prettyFormatGroupBy(buf, node)
	case OrderBy:
		prettyFormatOrderBy(buf, node)
	case *Limit:
		prettyFormatLimit(buf, node)
	default:
		node.Format(buf)
	}
}

func prettyFormatSelect(buf *TrackedBuffer, node *Select) {
	if node == nil {
		return
	}

	if len(node.Comments) > 0 {
		comments := strings.TrimSpace(String(node.Comments, false))
		if comments != "" {
			buf.Myprintf("%s\n", comments)
		}
	}

	buf.WriteString("select")
	prefixLen := len("select")
	appendClause := func(value string) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return
		}
		buf.Myprintf(" %s", trimmed)
		prefixLen += 1 + len(trimmed)
	}

	appendClause(node.Cache)
	appendClause(node.Distinct)
	appendClause(node.Hints)

	if len(node.SelectExprs) > 0 {
		indent := strings.Repeat(" ", prefixLen+1)
		buf.WriteByte(' ')
		for i, expr := range node.SelectExprs {
			if i > 0 {
				buf.Myprintf(",\n%s", indent)
			}
			buf.Myprintf("%v", expr)
		}
	}

	if len(node.From) > 0 {
		buf.WriteString("\nfrom ")
		for i, table := range node.From {
			if i > 0 {
				buf.WriteString(", ")
			}
			if pretty, ok := prettyTableExpr(table); ok {
				buf.WriteString(pretty)
				continue
			}
			buf.Myprintf("%v", table)
		}
	}

	if node.Where != nil && node.Where.Expr != nil {
		buf.Myprintf("\n%s %v", node.Where.Type, node.Where.Expr)
	}

	if len(node.GroupBy) > 0 {
		buf.WriteString("\ngroup by ")
		for i, expr := range node.GroupBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.Myprintf("%v", expr)
		}
	}

	if node.Having != nil && node.Having.Expr != nil {
		buf.Myprintf("\n%s %v", node.Having.Type, node.Having.Expr)
	}

	if len(node.OrderBy) > 0 {
		buf.WriteString("\norder by ")
		for i, expr := range node.OrderBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.Myprintf("%v", expr)
		}
	}

	if node.Limit != nil {
		buf.WriteString("\nlimit ")
		if node.Limit.Offset != nil {
			buf.Myprintf("%v, ", node.Limit.Offset)
		}
		buf.Myprintf("%v", node.Limit.Rowcount)
	}

	if node.Lock != "" {
		lock := strings.TrimSpace(node.Lock)
		if lock != "" {
			buf.Myprintf("\n%s", lock)
		}
	}
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
