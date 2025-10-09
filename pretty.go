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
		prettyFormatClause(buf, node)
	case GroupBy:
		prettyFormatClause(buf, node)
	case OrderBy:
		prettyFormatClause(buf, node)
	case *Limit:
		prettyFormatClause(buf, node)
	case *AliasedTableExpr:
		prettyFormatAliasedTableExpr(buf, node)
	case *Subquery:
		prettyFormatSubquery(buf, node)
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
			buf.Myprintf("%v", table)
		}
	}

	prettyFormatClause(buf, node.Where)
	prettyFormatClause(buf, node.GroupBy)
	prettyFormatClause(buf, node.Having)
	prettyFormatClause(buf, node.OrderBy)
	prettyFormatClause(buf, node.Limit)

	if node.Lock != "" {
		lock := strings.TrimSpace(node.Lock)
		if lock != "" {
			buf.Myprintf("\n%s", lock)
		}
	}
}

func prettyFormatClause(buf *TrackedBuffer, node SQLNode) {
	if node == nil {
		return
	}
	origLen := buf.Len()
	if origLen > 0 {
		buf.WriteByte('\n')
	}
	clauseStart := buf.Len()
	origFormatter := buf.nodeFormatter
	buf.nodeFormatter = nil
	node.Format(buf)
	buf.nodeFormatter = origFormatter
	if buf.Len() == clauseStart {
		if origLen > 0 {
			buf.Truncate(origLen)
		}
		return
	}
	data := buf.Bytes()
	if clauseStart < len(data) && data[clauseStart] == ' ' {
		copy(data[clauseStart:], data[clauseStart+1:])
		buf.Truncate(len(data) - 1)
	}
}

func prettyFormatAliasedTableExpr(buf *TrackedBuffer, node *AliasedTableExpr) {
	if node == nil {
		return
	}
	buf.Myprintf("%v%v", node.Expr, node.Partitions)
	if !node.As.IsEmpty() {
		buf.Myprintf(" as %v", node.As)
	}
	if node.Hints != nil {
		buf.Myprintf("%v", node.Hints)
	}
}

func prettyFormatSubquery(buf *TrackedBuffer, node *Subquery) {
	if node == nil || node.Select == nil {
		buf.WriteString("()")
		return
	}
	inner := String(node.Select, true)
	if inner == "" {
		buf.WriteString("()")
		return
	}
	buf.WriteString("(\n")
	buf.WriteString(indentLines(inner, "\t"))
	buf.WriteString("\n)")
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
