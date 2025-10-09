package sqlparser

import (
	"fmt"
	"io"
	"strings"
)

func RewriteSqls(sql string) (string, error) {
	tokenizer := NewStringTokenizer(sql)
	var rewritten []string
	for {
		stmt, err := ParseNext(tokenizer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("ParseNext error: %w", err)
		}
		if stmt == nil {
			continue
		}
		selectStmt, ok := stmt.(*Select)
		if !ok {
			return "", fmt.Errorf("unexpected statement type %T", stmt)
		}
		if err := rewriteSql(selectStmt); err != nil {
			return "", err
		}
		rewritten = append(rewritten, String(selectStmt, true))
	}

	return strings.Join(rewritten, ";\n"), nil
}

func rewriteSql(sel *Select) error {
	if rewritten, err := rewriteEdgeSql(sel); err != nil {
		return err
	} else if rewritten {
		return nil
	}
	if rewritten, err := rewritePointSql(sel); err != nil {
		return err
	} else if rewritten {
		return nil
	}

	return fmt.Errorf("select does not contain recognizable point or edge columns")
}

func rewriteEdgeSql(sel *Select) (bool, error) {
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
		return false, nil
	}

	if point1ID == nil || point2ID == nil || point1Type == nil || point2Type == nil {
		return false, fmt.Errorf("missing required columns: p1=%t p2=%t p1Type=%t p2Type=%t", point1ID != nil, point2ID != nil, point1Type != nil, point2Type != nil)
	}

	point1Expr, err := newAliasedExprFromString(fmt.Sprintf("named_struct('id', cast(%s as string))", String(point1ID.Expr, false)), "outv_pk_prop")
	if err != nil {
		return false, err
	}
	selectExprs := SelectExprs{point1Expr}

	point2Expr, err := newAliasedExprFromString(fmt.Sprintf("cast(%s as string)", String(point2ID.Expr, false)), "bg__id")
	if err != nil {
		return false, err
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
	return true, nil
}

func rewritePointSql(sel *Select) (bool, error) {
	var (
		pointID   *AliasedExpr
		pointType *AliasedExpr
		remaining SelectExprs
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
		return false, nil
	}

	if pointID == nil || pointType == nil {
		return false, fmt.Errorf("missing required point columns: point_id=%t point_type=%t", pointID != nil, pointType != nil)
	}

	pointType.As = NewColIdent("label")

	pointIDExpr, err := newAliasedExprFromString(fmt.Sprintf("cast(%s as string)", String(pointID.Expr, false)), "id")
	if err != nil {
		return false, err
	}
	selectExprs := SelectExprs{pointType, pointIDExpr}

	for _, expr := range remaining {
		selectExprs = append(selectExprs, expr)
	}

	sel.SelectExprs = selectExprs
	return true, nil
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
