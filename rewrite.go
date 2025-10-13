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

func RewriteSqls(sql string, pretty bool, typeMap map[string]string) (map[string]*SqlDef, error) {
	if len(strings.TrimSpace(sql)) == 0 {
		return nil, nil
	}
	tokenizer := NewStringTokenizer(sql)
	rewritten := make(map[string]*SqlDef)
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
		key, err := rewriteSql(selectStmt, typeMap)
		if err != nil {
			return nil, err
		}
		rendered := strings.Replace(String(stmt, pretty)+";", "'", "\"", -1)
		if existing, ok := rewritten[key]; ok {
			rewritten[key].Sql = existing.Sql + "\n" + rendered
		} else {
			rewritten[key] = &SqlDef{
				Sql:       rendered,
				LabelType: "string",
			}
		}
	}

	return rewritten, nil
}

func rewriteSql(sel *Select, typeMap map[string]string) (string, error) {
	if key, rewritten, err := rewriteEdgeSql(sel, typeMap); err != nil {
		return "", err
	} else if rewritten {
		return key, nil
	}
	if key, rewritten, err := rewritePointSql(sel, typeMap); err != nil {
		return "", err
	} else if rewritten {
		return key, nil
	}

	return "", fmt.Errorf("select does not contain recognizable point or edge columns")
}

func rewriteEdgeSql(sel *Select, typeMap map[string]string) (string, bool, error) {
	edgeTypeLiteral, _ := findStringLiteralForAliasInSelect(sel, "edge_type")

	var (
		point1ID   *AliasedExpr
		point2ID   *AliasedExpr
		point1Type *AliasedExpr
		point2Type *AliasedExpr
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
		case "value", "ts_us":
			// value and ts_us are deprecated and should be dropped.
		default:
			remaining = append(remaining, aliased)
		}
	}

	if point1ID == nil && point2ID == nil && point1Type == nil && point2Type == nil {
		return "", false, nil
	}

	if point1ID == nil || point2ID == nil || point1Type == nil || point2Type == nil {
		return "", false, fmt.Errorf("missing required columns: p1=%t p2=%t p1Type=%t p2Type=%t", point1ID != nil, point2ID != nil, point1Type != nil, point2Type != nil)
	}

	point1Expr, err := newAliasedExprFromString(fmt.Sprintf("named_struct('id', cast(%s as string))", String(point1ID.Expr, false)), "outv_pk_prop")
	if err != nil {
		return "", false, err
	}
	selectExprs := SelectExprs{point1Expr}

	point2Expr, err := newAliasedExprFromString(fmt.Sprintf("cast(%s as string)", String(point2ID.Expr, false)), "bg__id")
	if err != nil {
		return "", false, err
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

		name := aliasOrColumnName(aliased)
		switch name {
		case "edge_type":
			aliased.As = NewColIdent("label")
			if edgeTypeLiteral == "" {
				if literal, err := extractStringLiteral(aliased.Expr); err == nil {
					edgeTypeLiteral = literal
				}
			}
			selectExprs = append(selectExprs, aliased)
			continue
		}

		if aliased.As.IsEmpty() {
			if derived := deriveAliasFromExpr(aliased.Expr); derived != "" {
				aliased.As = NewColIdent(derived)
			}
		}

		selectExprs = append(selectExprs, aliased)
	}

	sel.SelectExprs = selectExprs
	if err := applyTypeAnnotations(sel.SelectExprs, typeMap); err != nil {
		return "", false, err
	}
	if edgeTypeLiteral == "" {
		return "", false, fmt.Errorf("edge sql missing literal edge_type column")
	}
	return edgeTypeLiteral, true, nil
}

func rewritePointSql(sel *Select, typeMap map[string]string) (string, bool, error) {
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
		return "", false, nil
	}

	if pointID == nil || pointType == nil {
		return "", false, fmt.Errorf("missing required point columns: point_id=%t point_type=%t", pointID != nil, pointType != nil)
	}

	literal, err := extractStringLiteral(pointType.Expr)
	if err != nil {
		if fallback, ok := findStringLiteralForAliasInSelect(sel, "point_type"); ok {
			pointTypeLiteral = fallback
		} else {
			return "", false, err
		}
	} else {
		pointTypeLiteral = literal
	}

	pointType.As = NewColIdent("label")

	pointIDExpr, err := newAliasedExprFromString(fmt.Sprintf("cast(%s as string)", String(pointID.Expr, false)), "id")
	if err != nil {
		return "", false, err
	}
	selectExprs := SelectExprs{pointType, pointIDExpr}

	for _, expr := range remaining {
		selectExprs = append(selectExprs, expr)
	}

	sel.SelectExprs = selectExprs
	if err := applyTypeAnnotations(sel.SelectExprs, typeMap); err != nil {
		return "", false, err
	}
	return pointTypeLiteral, true, nil
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

		exprSQL := String(baseExpr, false)
		castExpr, err := mustParseExpr(fmt.Sprintf("cast(%s as %s)", exprSQL, targetType))
		if err != nil {
			return err
		}
		aliased.Expr = castExpr
	}
	return nil
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
			if sel, ok := subquery.Select.(*Select); ok {
				if literal, ok := findStringLiteralForAliasInSelect(sel, alias); ok {
					return literal, true
				}
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

func aliasOrColumnName(ae *AliasedExpr) string {
	if ae == nil {
		return ""
	}
	if !ae.As.IsEmpty() {
		return ae.As.Lowered()
	}
	if col, ok := ae.Expr.(*ColName); ok && col.Qualifier.IsEmpty() {
		return col.Name.Lowered()
	}
	return ""
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
		if e.Qualifier.IsEmpty() {
			return e.Name.Lowered()
		}
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
