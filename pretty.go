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
		prettyFormatWhereClause(buf, node)
	case GroupBy:
		prettyFormatGroupByClause(buf, node)
	case OrderBy:
		prettyFormatOrderByClause(buf, node)
	case *Limit:
		prettyFormatLimitClause(buf, node)
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
		ensureClauseNewline(buf)
		buf.Myprintf("from %v", node.From)
	}

	if node.Where != nil && node.Where.Expr != nil {
		ensureClauseNewline(buf)
		buf.Myprintf("%v", node.Where)
	}

	if len(node.GroupBy) > 0 {
		ensureClauseNewline(buf)
		buf.Myprintf("%v", node.GroupBy)
	}

	if node.Having != nil && node.Having.Expr != nil {
		ensureClauseNewline(buf)
		buf.Myprintf("%v", node.Having)
	}

	if len(node.OrderBy) > 0 {
		ensureClauseNewline(buf)
		buf.Myprintf("%v", node.OrderBy)
	}

	if node.Limit != nil {
		ensureClauseNewline(buf)
		buf.Myprintf("%v", node.Limit)
	}

	if node.Lock != "" {
		lock := strings.TrimSpace(node.Lock)
		if lock != "" {
			buf.Myprintf("\n%s", lock)
		}
	}
}

func prettyFormatWhereClause(buf *TrackedBuffer, node *Where) {
	if node == nil || node.Expr == nil {
		return
	}

	ensureClauseNewline(buf)
	keyword := strings.TrimSpace(node.Type)
	if keyword == "" {
		keyword = WhereStr
	}

	prettyFormatLogicalClause(buf, keyword, node.Expr)
}

func prettyFormatLogicalClause(buf *TrackedBuffer, keyword string, expr Expr) {
	buf.WriteString(keyword)
	buf.WriteByte(' ')

	op, terms := flattenBooleanExpr(expr)
	if op == "" || len(terms) <= 1 {
		buf.Myprintf("%v", expr)
		return
	}

	buf.Myprintf("%v", terms[0])
	exprIndent := len(keyword) + 1
	padding := exprIndent - (len(op) + 1)
	if padding < 1 {
		padding = 1
	}
	pad := strings.Repeat(" ", padding)

	for _, term := range terms[1:] {
		buf.WriteByte('\n')
		buf.WriteString(op)
		buf.WriteByte(' ')
		buf.WriteString(pad)
		buf.Myprintf("%v", term)
	}
}

func flattenBooleanExpr(expr Expr) (string, []Expr) {
	switch expr.(type) {
	case *AndExpr:
		return "and", flattenBinaryExpr(expr, "and")
	case *OrExpr:
		return "or", flattenBinaryExpr(expr, "or")
	default:
		return "", []Expr{expr}
	}
}

func flattenBinaryExpr(expr Expr, op string) []Expr {
	switch node := expr.(type) {
	case *AndExpr:
		if op == "and" {
			terms := flattenBinaryExpr(node.Left, op)
			return append(terms, flattenBinaryExpr(node.Right, op)...)
		}
	case *OrExpr:
		if op == "or" {
			terms := flattenBinaryExpr(node.Left, op)
			return append(terms, flattenBinaryExpr(node.Right, op)...)
		}
	}
	return []Expr{expr}
}

func prettyFormatGroupByClause(buf *TrackedBuffer, node GroupBy) {
	if len(node) == 0 {
		return
	}

	ensureClauseNewline(buf)
	buf.WriteString("group by ")
	buf.Myprintf("%v", node[0])
	indent := strings.Repeat(" ", len("group by "))
	for i := 1; i < len(node); i++ {
		buf.WriteString(",\n")
		buf.WriteString(indent)
		buf.Myprintf("%v", node[i])
	}
}

func prettyFormatOrderByClause(buf *TrackedBuffer, node OrderBy) {
	if len(node) == 0 {
		return
	}

	ensureClauseNewline(buf)
	buf.WriteString("order by ")
	buf.Myprintf("%v", node[0])
	indent := strings.Repeat(" ", len("order by "))
	for i := 1; i < len(node); i++ {
		buf.WriteString(",\n")
		buf.WriteString(indent)
		buf.Myprintf("%v", node[i])
	}
}

func prettyFormatLimitClause(buf *TrackedBuffer, node *Limit) {
	if node == nil {
		return
	}

	ensureClauseNewline(buf)
	buf.WriteString("limit ")
	if node.Offset != nil {
		buf.Myprintf("%v, ", node.Offset)
	}
	buf.Myprintf("%v", node.Rowcount)
}

func ensureClauseNewline(buf *TrackedBuffer) {
	if buf == nil {
		return
	}
	if buf.Len() == 0 {
		return
	}
	data := buf.Bytes()
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return
	}
	buf.WriteByte('\n')
}

func prettyFormatSubquery(buf *TrackedBuffer, node *Subquery) {
	if node == nil || node.Select == nil {
		buf.WriteString("()")
		return
	}
	inner := NewTrackedBuffer(buf.nodeFormatter)
	inner.Myprintf("%v", node.Select)
	innerSQL := inner.String()
	if innerSQL == "" {
		buf.WriteString("()")
		return
	}
	buf.WriteString("(\n")
	buf.WriteString(indentLines(innerSQL, "\t"))
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
