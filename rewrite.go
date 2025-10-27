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

func RewriteSqls(sql string, pretty bool, typeMap map[string]map[string]string) (map[string]*SqlDef, error) {
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
		statements, err := expandEdgeTypeStatements(stmt)
		if err != nil {
			return nil, err
		}
		for _, current := range statements {
			unwrapped := current
			if withStmt, ok := current.(*With); ok {
				unwrapped = withStmt.Stmt
			}
			selectStmt, ok := unwrapped.(*Select)
			if !ok {
				return nil, fmt.Errorf("unexpected statement type %T", current)
			}
			key, err := rewriteSql(selectStmt, typeMap)
			if err != nil {
				return nil, err
			}
			rendered := strings.Replace(String(current, pretty)+";", "'", "\"", -1)
			if existing, ok := rewritten[key]; ok {
				rewritten[key].Sql = existing.Sql + "\n" + rendered
			} else {
				rewritten[key] = &SqlDef{
					Sql:       rendered,
					LabelType: "string",
				}
			}
		}
	}

	return rewritten, nil
}

func expandEdgeTypeStatements(stmt Statement) ([]Statement, error) {
	selectStmt, ok := selectFromStatement(stmt)
	if !ok {
		return []Statement{stmt}, nil
	}
	variants, err := extractDynamicEdgeTypeVariants(selectStmt)
	if err != nil {
		return nil, err
	}
	if len(variants) == 0 {
		return []Statement{stmt}, nil
	}

	baseSQL := String(stmt, false)
	statements := make([]Statement, 0, len(variants))
	for _, variant := range variants {
		tokenizer := NewStringTokenizer(baseSQL)
		parsed, err := ParseNext(tokenizer)
		if err != nil {
			return nil, fmt.Errorf("ParseNext error while expanding edge_type: %w", err)
		}
		if _, err := ParseNext(tokenizer); err != io.EOF {
			if err == nil {
				return nil, fmt.Errorf("expected single statement while expanding edge_type")
			}
			return nil, fmt.Errorf("ParseNext error while validating expanded statement: %w", err)
		}
		selectCopy, ok := selectFromStatement(parsed)
		if !ok {
			return nil, fmt.Errorf("unexpected statement type %T while expanding edge_type", parsed)
		}
		if err := applyEdgeTypeVariant(selectCopy, variant); err != nil {
			return nil, err
		}
		statements = append(statements, parsed)
	}

	return statements, nil
}

func rewriteSql(sel *Select, typeMap map[string]map[string]string) (string, error) {
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

func rewriteEdgeSql(sel *Select, typeMap map[string]map[string]string) (string, bool, error) {
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
		case "value":
			// value is deprecated and should be dropped.
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

		switch aliasOrColumnName(aliased) {
		case "edge_type":
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

	rowSelect := &Select{
		SelectExprs: SelectExprs{
			&StarExpr{},
			&AliasedExpr{
				Expr: &FuncExpr{
					Name: NewColIdent("row_number"),
					Over: &WindowSpecification{
						PartitionBy: Exprs{
							point1ID.Expr,
							point2ID.Expr,
							point1Type.Expr,
							point2Type.Expr,
						},
						OrderBy: OrderBy{
							&Order{
								Expr: NewIntVal([]byte("1")),
							},
						},
					},
				},
				As: NewColIdent("rn"),
			},
		},
		From: sel.From,
	}
	sel.From = TableExprs{
		&AliasedTableExpr{
			Expr: &Subquery{Select: rowSelect},
		},
	}
	sel.AddWhere(&ComparisonExpr{
		Operator: EqualStr,
		Left: &ColName{
			Name: NewColIdent("rn"),
		},
		Right: NewIntVal([]byte("1")),
	})

	var columnTypes map[string]string
	if edgeTypeLiteral != "" {
		columnTypes = typeMap[edgeTypeLiteral]
	}
	if err := applyTypeAnnotations(sel.SelectExprs, columnTypes); err != nil {
		return "", false, err
	}
	if edgeTypeLiteral == "" {
		return "", false, fmt.Errorf("edge sql missing literal edge_type column")
	}
	return edgeTypeLiteral, true, nil
}

type dynamicEdgeVariant struct {
	Column   string
	Value    string
	EdgeType string
}

func selectFromStatement(stmt Statement) (*Select, bool) {
	switch s := stmt.(type) {
	case *Select:
		return s, true
	case *With:
		if selectStmt, ok := s.Stmt.(*Select); ok {
			return selectStmt, true
		}
	}
	return nil, false
}

func extractDynamicEdgeTypeVariants(sel *Select) ([]dynamicEdgeVariant, error) {
	edgeExpr := findAliasedExprInSelect(sel, "edge_type")
	if edgeExpr == nil {
		return nil, nil
	}
	if literal, ok := findStringLiteralForAliasInSelect(sel, "edge_type"); ok && literal != "" {
		return nil, nil
	}
	if _, err := extractStringLiteral(edgeExpr.Expr); err == nil {
		return nil, nil
	}

	columns := make(map[string]struct{})
	collectColumnNames(edgeExpr.Expr, columns)
	if len(columns) != 1 {
		return nil, nil
	}

	var columnName string
	for name := range columns {
		columnName = name
	}
	values, ok := findStringValuesForColumn(sel.Where, columnName)
	if !ok || len(values) == 0 {
		return nil, fmt.Errorf("unable to determine values for column %s used in dynamic edge_type", columnName)
	}

	variants := make([]dynamicEdgeVariant, 0, len(values))
	for _, value := range values {
		edgeType, err := evaluateEdgeType(edgeExpr.Expr, map[string]string{columnName: value})
		if err != nil {
			return nil, err
		}
		variants = append(variants, dynamicEdgeVariant{
			Column:   columnName,
			Value:    value,
			EdgeType: edgeType,
		})
	}

	return variants, nil
}

func findAliasedExprInSelect(sel *Select, alias string) *AliasedExpr {
	for _, expr := range sel.SelectExprs {
		aliased, ok := expr.(*AliasedExpr)
		if !ok {
			continue
		}
		if aliasOrColumnName(aliased) == alias {
			return aliased
		}
	}
	return nil
}

func collectColumnNames(expr Expr, columns map[string]struct{}) {
	switch e := expr.(type) {
	case *ColName:
		columns[e.Name.Lowered()] = struct{}{}
	case *FuncExpr:
		for _, arg := range e.Exprs {
			if aliased, ok := arg.(*AliasedExpr); ok {
				collectColumnNames(aliased.Expr, columns)
			}
		}
	case *BinaryExpr:
		collectColumnNames(e.Left, columns)
		collectColumnNames(e.Right, columns)
	case *ConvertExpr:
		collectColumnNames(e.Expr, columns)
	case *ParenExpr:
		collectColumnNames(e.Expr, columns)
	}
}

func findStringValuesForColumn(where *Where, column string) ([]string, bool) {
	if where == nil {
		return nil, false
	}
	return findStringValuesInExpr(where.Expr, column)
}

func findStringValuesInExpr(expr Expr, column string) ([]string, bool) {
	switch e := expr.(type) {
	case *AndExpr:
		if values, ok := findStringValuesInExpr(e.Left, column); ok {
			return values, true
		}
		return findStringValuesInExpr(e.Right, column)
	case *ParenExpr:
		return findStringValuesInExpr(e.Expr, column)
	case *ComparisonExpr:
		if !columnMatches(e.Left, column) {
			return nil, false
		}
		switch e.Operator {
		case InStr:
			tuple, ok := e.Right.(ValTuple)
			if !ok {
				return nil, false
			}
			values := make([]string, 0, len(tuple))
			for _, valExpr := range tuple {
				literal, err := extractStringLiteral(valExpr)
				if err != nil {
					return nil, false
				}
				values = append(values, literal)
			}
			return values, true
		case EqualStr:
			literal, err := extractStringLiteral(e.Right)
			if err != nil {
				return nil, false
			}
			return []string{literal}, true
		}
	}
	return nil, false
}

func columnMatches(expr Expr, column string) bool {
	col, ok := expr.(*ColName)
	if !ok {
		return false
	}
	return col.Name.Lowered() == column
}

func evaluateEdgeType(expr Expr, values map[string]string) (string, error) {
	switch e := expr.(type) {
	case *SQLVal:
		if e.Type != StrVal {
			return "", fmt.Errorf("edge_type expression contains non-string literal %T", expr)
		}
		return string(e.Val), nil
	case *ColName:
		value, ok := values[e.Name.Lowered()]
		if !ok {
			return "", fmt.Errorf("missing value for column %s in edge_type expression", e.Name.Lowered())
		}
		return value, nil
	case *FuncExpr:
		if e.Name.Lowered() != "concat" {
			return "", fmt.Errorf("unsupported function %s in edge_type expression", e.Name.String())
		}
		var parts []string
		for _, arg := range e.Exprs {
			aliased, ok := arg.(*AliasedExpr)
			if !ok {
				return "", fmt.Errorf("unexpected concat argument type %T", arg)
			}
			part, err := evaluateEdgeType(aliased.Expr, values)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		return strings.Join(parts, ""), nil
	case *BinaryExpr:
		left, err := evaluateEdgeType(e.Left, values)
		if err != nil {
			return "", err
		}
		right, err := evaluateEdgeType(e.Right, values)
		if err != nil {
			return "", err
		}
		return left + right, nil
	case *ConvertExpr:
		return evaluateEdgeType(e.Expr, values)
	case *ParenExpr:
		return evaluateEdgeType(e.Expr, values)
	}
	return "", fmt.Errorf("unsupported expression type %T in edge_type expression", expr)
}

func applyEdgeTypeVariant(sel *Select, variant dynamicEdgeVariant) error {
	edgeExpr := findAliasedExprInSelect(sel, "edge_type")
	if edgeExpr == nil {
		return fmt.Errorf("edge_type column not found while applying variant")
	}
	edgeExpr.Expr = NewStrVal([]byte(variant.EdgeType))

	if sel.Where == nil {
		return fmt.Errorf("unable to constrain column %s: missing WHERE clause", variant.Column)
	}
	if !applyValueToColumnInExpr(sel.Where.Expr, variant.Column, variant.Value) {
		return fmt.Errorf("failed to constrain column %s in WHERE clause", variant.Column)
	}
	return nil
}

func applyValueToColumnInExpr(expr Expr, column, value string) bool {
	switch e := expr.(type) {
	case *AndExpr:
		if applyValueToColumnInExpr(e.Left, column, value) {
			return true
		}
		return applyValueToColumnInExpr(e.Right, column, value)
	case *ParenExpr:
		return applyValueToColumnInExpr(e.Expr, column, value)
	case *ComparisonExpr:
		if !columnMatches(e.Left, column) {
			return false
		}
		e.Right = NewStrVal([]byte(value))
		e.Operator = EqualStr
		return true
	}
	return false
}

func rewritePointSql(sel *Select, typeMap map[string]map[string]string) (string, bool, error) {
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
	var columnTypes map[string]string
	if pointTypeLiteral != "" {
		columnTypes = typeMap[pointTypeLiteral]
	}
	if err := applyTypeAnnotations(sel.SelectExprs, columnTypes); err != nil {
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

		aliased.Expr = &ConvertExpr{
			Expr: baseExpr,
			Type: &ConvertType{Type: targetType},
			Cast: true,
		}
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
