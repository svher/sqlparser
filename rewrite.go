package sqlparser

import (
	"fmt"
	"io"
	"strings"
)

type SqlDef struct {
	Sql       string `json:"sql"`
	LabelType string `json:"label_type"`
}

type rewriteResult struct {
	statement    Statement
	selectStmt   *Select
	dedupColumns []string
}

func RewriteSqls(sql string, pretty bool, typeMap map[string]map[string]string) (map[string]*SqlDef, error) {
	if len(strings.TrimSpace(sql)) == 0 {
		return nil, nil
	}
	tokenizer := NewStringTokenizer(sql)
	rewritten := make(map[string]*SqlDef)
	grouped := make(map[string][]*rewriteResult)
	appendResult := func(key string, result *rewriteResult) {
		grouped[key] = append(grouped[key], result)
	}
	for {
		stmt, err := ParseNext(tokenizer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ParseNext error: %w", err)
		}
		if stmt == nil {
			continue
		}
		unwrapped := stmt
		if withStmt, ok := stmt.(*With); ok {
			unwrapped = withStmt.Stmt
		}
		selectStmt, ok := unwrapped.(*Select)
		if !ok {
			return nil, fmt.Errorf("unexpected statement type %T", stmt)
		}
		key, dedupCols, err := rewriteSql(stmt, selectStmt, typeMap)
		if err != nil {
			return nil, err
		}
		appendResult(key, &rewriteResult{
			statement:    stmt,
			selectStmt:   selectStmt,
			dedupColumns: dedupCols,
		})
	}

	for key, results := range grouped {
		stmt, err := finalizeRewriteGroup(results)
		if err != nil {
			return nil, err
		}

		rewritten[key] = &SqlDef{
			Sql:       strings.Replace(String(stmt, pretty)+";", "'", "\"", -1),
			LabelType: "string",
		}
	}

	return rewritten, nil
}

func rewriteSql(stmt Statement, sel *Select, typeMap map[string]map[string]string) (string, []string, error) {
	if key, dedupCols, rewritten, err := rewriteEdgeSql(sel, typeMap); err != nil {
		return "", nil, err
	} else if rewritten {
		return key, dedupCols, nil
	}
	if key, dedupCols, rewritten, err := rewritePointSql(sel, typeMap); err != nil {
		return "", nil, err
	} else if rewritten {
		return key, dedupCols, nil
	}

	return "", nil, fmt.Errorf("select does not contain recognizable point or edge columns")
}

func rewriteEdgeSql(sel *Select, typeMap map[string]map[string]string) (string, []string, bool, error) {
	edgeTypeLiteral, _ := findStringLiteralForAliasInSelect(sel, "edge_type")

	var (
		point1ID   *AliasedExpr
		point2ID   *AliasedExpr
		point1Type *AliasedExpr
		point2Type *AliasedExpr
		edgeType   *AliasedExpr
		remaining  SelectExprs
	)

	for _, expr := range sel.SelectExprs {
		aliased, ok := expr.(*AliasedExpr)
		if !ok {
			remaining = append(remaining, expr)
			continue
		}

		switch aliasOrColumnName(aliased) {
		case "point1_id":
			point1ID = aliased
		case "point2_id":
			point2ID = aliased
		case "point1_type":
			point1Type = aliased
		case "point2_type":
			point2Type = aliased
		case "value":
			// value is deprecated and should be dropped.
		default:
			remaining = append(remaining, aliased)
		}
	}

	if point1ID == nil && point2ID == nil && point1Type == nil && point2Type == nil {
		return "", nil, false, nil
	}

	if point1ID == nil || point2ID == nil || point1Type == nil || point2Type == nil {
		return "", nil, false, fmt.Errorf("missing required columns: p1=%t p2=%t p1Type=%t p2Type=%t", point1ID != nil, point2ID != nil, point1Type != nil, point2Type != nil)
	}

	point1Expr, err := newAliasedExprFromString(fmt.Sprintf("named_struct('id', cast(%s as string))", String(point1ID.Expr, false)), "outv_pk_prop")
	if err != nil {
		return "", nil, false, err
	}
	selectExprs := SelectExprs{point1Expr}

	point2Expr, err := newAliasedExprFromString(fmt.Sprintf("cast(%s as string)", String(point2ID.Expr, false)), "bg__id")
	if err != nil {
		return "", nil, false, err
	}
	selectExprs = append(selectExprs, point2Expr)

	point1Type.As = NewColIdent("outv_label")
	selectExprs = append(selectExprs, point1Type)

	point2Type.As = NewColIdent("bg__bg__label")
	selectExprs = append(selectExprs, point2Type)

	for _, expr := range remaining {
		aliased, ok := expr.(*AliasedExpr)
		if !ok {
			selectExprs = append(selectExprs, expr)
			continue
		}

		switch aliasOrColumnName(aliased) {
		case "edge_type":
			edgeType = aliased
			aliased.As = NewColIdent("label")
			if edgeTypeLiteral == "" {
				if literal, err := extractStringLiteral(aliased.Expr); err == nil {
					edgeTypeLiteral = literal
				}
			}
			selectExprs = append(selectExprs, aliased)
		default:
			selectExprs = append(selectExprs, aliased)
		}
	}

	sel.SelectExprs = selectExprs

	if edgeTypeLiteral == "" && edgeType != nil {
		if literal, ok := deriveEdgeTypeLiteralFromExpr(sel, edgeType.Expr); ok {
			edgeTypeLiteral = literal
		}
	}
	if edgeTypeLiteral == "" {
		return "", nil, false, fmt.Errorf("edge sql missing literal edge_type column")
	}
	if err := applyTypeAnnotations(sel.SelectExprs, typeMap[edgeTypeLiteral]); err != nil {
		return "", nil, false, err
	}

	return edgeTypeLiteral, []string{"outv_pk_prop", "bg__id", "outv_label", "bg__bg__label"}, true, nil
}

func rewritePointSql(sel *Select, typeMap map[string]map[string]string) (string, []string, bool, error) {
	var (
		pointID          *AliasedExpr
		pointType        *AliasedExpr
		pointTypeLiteral string
		remaining        SelectExprs
	)

	for _, expr := range sel.SelectExprs {
		aliased, ok := expr.(*AliasedExpr)
		if !ok {
			remaining = append(remaining, expr)
			continue
		}

		switch aliasOrColumnName(aliased) {
		case "point_id":
			pointID = aliased
		case "point_type":
			pointType = aliased
		case "point_value":
			// deprecated
		default:
			remaining = append(remaining, aliased)
		}
	}

	if pointID == nil && pointType == nil {
		return "", nil, false, nil
	}

	if pointID == nil || pointType == nil {
		return "", nil, false, fmt.Errorf("missing required point columns: point_id=%t point_type=%t", pointID != nil, pointType != nil)
	}

	literal, err := extractStringLiteral(pointType.Expr)
	if err != nil {
		if fallback, ok := findStringLiteralForAliasInSelect(sel, "point_type"); ok {
			pointTypeLiteral = fallback
		} else {
			return "", nil, false, err
		}
	} else {
		pointTypeLiteral = literal
	}

	pointType.As = NewColIdent("label")

	pointIDExpr, err := newAliasedExprFromString(fmt.Sprintf("cast(%s as string)", String(pointID.Expr, false)), "id")
	if err != nil {
		return "", nil, false, err
	}
	selectExprs := SelectExprs{pointType, pointIDExpr}

	for _, expr := range remaining {
		selectExprs = append(selectExprs, expr)
	}

	sel.SelectExprs = selectExprs

	if pointTypeLiteral == "" {
		return "", nil, false, fmt.Errorf("point sql missing literal point_type column")
	}
	if err := applyTypeAnnotations(sel.SelectExprs, typeMap[pointTypeLiteral]); err != nil {
		return "", nil, false, err
	}

	return pointTypeLiteral, []string{"id", "label"}, true, nil
}

func finalizeRewriteGroup(results []*rewriteResult) (Statement, error) {
	dedupCols := results[0].dedupColumns

	for _, res := range results {
		if !stringSlicesEqual(res.dedupColumns, dedupCols) {
			return nil, fmt.Errorf("inconsistent dedup columns within rewrite group")
		}
	}

	unionStmt, projection, ok := buildUnionForResults(results)
	if !ok {
		return nil, fmt.Errorf("build union statement failed")
	}
	outerSelect := &Select{
		SelectExprs: projection,
		From: TableExprs{
			&AliasedTableExpr{
				Expr: &Subquery{Select: unionStmt},
			},
		},
	}
	applyDeduplication(outerSelect, columnNamesToExprs(dedupCols))
	return outerSelect, nil
}

func buildUnionForResults(results []*rewriteResult) (SelectStatement, SelectExprs, bool) {
	if len(results) == 0 {
		return nil, nil, false
	}

	var union SelectStatement
	for i, res := range results {
		stmt, ok := res.statement.(SelectStatement)
		if !ok || res.selectStmt == nil {
			return nil, nil, false
		}
		if i == 0 {
			union = stmt
			continue
		}
		union = &Union{
			Type:  UnionAllStr,
			Left:  union,
			Right: stmt,
		}
	}

	projection, err := buildUnionProjection(results[0].selectStmt)
	if err != nil {
		return nil, nil, false
	}

	return union, projection, true
}

func buildUnionProjection(sel *Select) (SelectExprs, error) {
	if sel == nil {
		return nil, fmt.Errorf("select is nil")
	}

	projection := make(SelectExprs, 0, len(sel.SelectExprs))
	for _, expr := range sel.SelectExprs {
		aliased, ok := expr.(*AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported select expression %T for union projection", expr)
		}
		name := aliasOrColumnName(aliased)
		if name == "" {
			return nil, fmt.Errorf("missing column name for union projection")
		}
		column := &AliasedExpr{
			Expr: &ColName{
				Name: NewColIdent(name),
			},
		}
		if !aliased.As.IsEmpty() && aliased.As.String() != name {
			column.As = aliased.As
		}
		projection = append(projection, column)
	}
	return projection, nil
}

func columnNamesToExprs(columns []string) Exprs {
	if len(columns) == 0 {
		return nil
	}
	exprs := make(Exprs, 0, len(columns))
	for _, name := range columns {
		if name == "" {
			continue
		}
		exprs = append(exprs, &ColName{
			Name: NewColIdent(name),
		})
	}
	return exprs
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func applyTypeAnnotations(selectExprs SelectExprs, typeMap map[string]string) error {
	if len(typeMap) == 0 {
		return nil
	}
	for _, expr := range selectExprs {
		aliased, ok := expr.(*AliasedExpr)
		if !ok {
			continue
		}

		name := aliasOrColumnName(aliased)
		targetType, ok := typeMap[name]
		if !ok {
			continue
		}

		baseExpr := aliased.Expr
		if convert, ok := baseExpr.(*ConvertExpr); ok {
			baseExpr = convert.Expr
		}

		aliased.Expr = &ConvertExpr{
			Expr: baseExpr,
			Type: &ConvertType{Type: targetType},
			Cast: true,
		}
	}
	return nil
}

func applyDeduplication(sel *Select, partitionExprs Exprs) {
	if len(partitionExprs) == 0 {
		return
	}

	outerSelectExprs := make(SelectExprs, 0, len(sel.SelectExprs))
	rowSelectExprs := make(SelectExprs, 0, len(sel.SelectExprs)+1)

	for i, expr := range sel.SelectExprs {
		switch e := expr.(type) {
		case *AliasedExpr:
			innerAlias := fmt.Sprintf("__vt_col%d", i)
			rowSelectExprs = append(rowSelectExprs, &AliasedExpr{
				Expr: e.Expr,
				As:   NewColIdent(innerAlias),
			})

			outerExpr := &AliasedExpr{
				Expr: &ColName{
					Name: NewColIdent(innerAlias),
				},
			}

			if name := aliasOrColumnName(e); name != "" {
				outerExpr.As = NewColIdent(name)
			}

			outerSelectExprs = append(outerSelectExprs, outerExpr)
		case *StarExpr:
			// Preserve star expressions by projecting them directly from the
			// deduplicated subquery.
			rowSelectExprs = append(rowSelectExprs, expr)
			outerSelectExprs = append(outerSelectExprs, &StarExpr{})
		default:
			// If we encounter an expression we do not know how to rewrite, skip
			// deduplication to avoid changing semantics.
			return
		}
	}

	rowSelectExprs = append(rowSelectExprs, &AliasedExpr{
		Expr: &FuncExpr{
			Name: NewColIdent("row_number"),
			Over: &WindowSpecification{
				PartitionBy: partitionExprs,
				OrderBy: OrderBy{
					&Order{
						Expr: NewIntVal([]byte("1")),
					},
				},
			},
		},
		As: NewColIdent("rn"),
	})

	rowSelect := &Select{
		SelectExprs: rowSelectExprs,
		From:        sel.From,
		Where:       sel.Where,
	}

	sel.SelectExprs = outerSelectExprs
	sel.From = TableExprs{
		&AliasedTableExpr{
			Expr: &Subquery{Select: rowSelect},
		},
	}
	sel.Where = nil
	sel.AddWhere(&ComparisonExpr{
		Operator: EqualStr,
		Left: &ColName{
			Name: NewColIdent("rn"),
		},
		Right: NewIntVal([]byte("1")),
	})
	sel.Distinct = ""
}

func extractStringLiteral(expr Expr) (string, error) {
	sqlVal, ok := expr.(*SQLVal)
	if !ok || sqlVal.Type != StrVal {
		return "", fmt.Errorf("expected string literal, got %T", expr)
	}
	return string(sqlVal.Val), nil
}

func findStringLiteralForAliasInSelect(sel *Select, alias string) (string, bool) {
	for _, expr := range sel.SelectExprs {
		aliased, ok := expr.(*AliasedExpr)
		if !ok {
			continue
		}
		if aliasOrColumnName(aliased) != alias {
			continue
		}
		if literal, err := extractStringLiteral(aliased.Expr); err == nil {
			return literal, true
		}
	}

	for _, tableExpr := range sel.From {
		if literal, ok := findStringLiteralForAliasInTableExpr(tableExpr, alias); ok {
			return literal, true
		}
	}

	return "", false
}

func findStringLiteralForAliasInTableExpr(tableExpr TableExpr, alias string) (string, bool) {
	switch expr := tableExpr.(type) {
	case *AliasedTableExpr:
		if subquery, ok := expr.Expr.(*Subquery); ok {
			if literal, ok := findStringLiteralForAliasInSelectStatement(subquery.Select, alias); ok {
				return literal, true
			}
		}
	case *ParenTableExpr:
		for _, innerExpr := range expr.Exprs {
			if literal, ok := findStringLiteralForAliasInTableExpr(innerExpr, alias); ok {
				return literal, true
			}
		}
	case *JoinTableExpr:
		if literal, ok := findStringLiteralForAliasInTableExpr(expr.LeftExpr, alias); ok {
			return literal, true
		}
		if literal, ok := findStringLiteralForAliasInTableExpr(expr.RightExpr, alias); ok {
			return literal, true
		}
	}
	return "", false
}

func findStringLiteralForAliasInSelectStatement(stmt SelectStatement, alias string) (string, bool) {
	switch s := stmt.(type) {
	case *Select:
		return findStringLiteralForAliasInSelect(s, alias)
	case *Union:
		if literal, ok := findStringLiteralForAliasInSelectStatement(s.Left, alias); ok {
			return literal, true
		}
		return findStringLiteralForAliasInSelectStatement(s.Right, alias)
	}
	return "", false
}

func deriveEdgeTypeLiteralFromExpr(sel *Select, expr Expr) (string, bool) {
	if sel == nil || sel.Where == nil || expr == nil {
		return "", false
	}

	bindings := make(map[string]string)
	collectStringColumnBindings(sel.Where.Expr, bindings)
	if len(bindings) == 0 {
		return "", false
	}

	literal, err := evaluateStringExpr(expr, bindings)
	if err != nil || literal == "" {
		return "", false
	}

	return literal, true
}

func collectStringColumnBindings(expr Expr, bindings map[string]string) {
	switch e := expr.(type) {
	case *AndExpr:
		collectStringColumnBindings(e.Left, bindings)
		collectStringColumnBindings(e.Right, bindings)
	case *ParenExpr:
		collectStringColumnBindings(e.Expr, bindings)
	case *ComparisonExpr:
		recordComparisonBinding(e, bindings)
	}
}

func recordComparisonBinding(expr *ComparisonExpr, bindings map[string]string) {
	if expr == nil {
		return
	}

	switch expr.Operator {
	case InStr:
		col, ok := expr.Left.(*ColName)
		if !ok {
			return
		}
		tuple, ok := expr.Right.(ValTuple)
		if !ok {
			return
		}
		for _, valExpr := range tuple {
			if literal, err := extractStringLiteral(valExpr); err == nil {
				recordStringBinding(col, literal, bindings)
				break
			}
		}
	case EqualStr:
		if col, literal, ok := resolveEqualityBinding(expr.Left, expr.Right); ok {
			recordStringBinding(col, literal, bindings)
		}
	}
}

func resolveEqualityBinding(left Expr, right Expr) (*ColName, string, bool) {
	if col, ok := left.(*ColName); ok {
		if literal, err := extractStringLiteral(right); err == nil {
			return col, literal, true
		}
	}
	if col, ok := right.(*ColName); ok {
		if literal, err := extractStringLiteral(left); err == nil {
			return col, literal, true
		}
	}
	return nil, "", false
}

func recordStringBinding(col *ColName, value string, bindings map[string]string) {
	if col == nil || value == "" {
		return
	}
	key := col.Name.Lowered()
	if key == "" {
		return
	}
	if _, exists := bindings[key]; !exists {
		bindings[key] = value
	}
}

func evaluateStringExpr(expr Expr, bindings map[string]string) (string, error) {
	switch e := expr.(type) {
	case *SQLVal:
		if e.Type != StrVal {
			return "", fmt.Errorf("unsupported sql value type %d", e.Type)
		}
		return string(e.Val), nil
	case *ColName:
		key := e.Name.Lowered()
		if key == "" {
			return "", fmt.Errorf("column name is empty")
		}
		value, ok := bindings[key]
		if !ok {
			return "", fmt.Errorf("missing binding for column %s", key)
		}
		return value, nil
	case *FuncExpr:
		switch e.Name.Lowered() {
		case "concat":
			var builder strings.Builder
			for _, arg := range e.Exprs {
				aliased, ok := arg.(*AliasedExpr)
				if !ok {
					return "", fmt.Errorf("unsupported concat argument type %T", arg)
				}
				part, err := evaluateStringExpr(aliased.Expr, bindings)
				if err != nil {
					return "", err
				}
				builder.WriteString(part)
			}
			return builder.String(), nil
		default:
			return "", fmt.Errorf("unsupported function %s", e.Name.String())
		}
	case *ConvertExpr:
		return evaluateStringExpr(e.Expr, bindings)
	case *ParenExpr:
		return evaluateStringExpr(e.Expr, bindings)
	}
	return "", fmt.Errorf("unsupported expression type %T", expr)
}

func aliasOrColumnName(ae *AliasedExpr) string {
	if ae == nil {
		return ""
	}
	if !ae.As.IsEmpty() {
		return ae.As.Lowered()
	}
	return deriveAliasFromExpr(ae.Expr)
}

func newAliasedExprFromString(expr, alias string) (*AliasedExpr, error) {
	parsedExpr, err := mustParseExpr(expr)
	if err != nil {
		return nil, err
	}
	return &AliasedExpr{
		Expr: parsedExpr,
		As:   NewColIdent(alias),
	}, nil
}

func deriveAliasFromExpr(expr Expr) string {
	switch e := expr.(type) {
	case *ColName:
		return e.Name.Lowered()
	case *ConvertExpr:
		return deriveAliasFromExpr(e.Expr)
	case *ParenExpr:
		return deriveAliasFromExpr(e.Expr)
	}
	return ""
}

func mustParseExpr(expr string) (Expr, error) {
	stmt, err := Parse("select " + expr)
	if err != nil {
		return nil, fmt.Errorf("parse expression %q: %w", expr, err)
	}
	sel, ok := stmt.(*Select)
	if !ok || len(sel.SelectExprs) != 1 {
		return nil, fmt.Errorf("unexpected expression parse tree for %q", expr)
	}
	aliased, ok := sel.SelectExprs[0].(*AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("expression %q did not parse as *AliasedExpr", expr)
	}
	return aliased.Expr, nil
}
