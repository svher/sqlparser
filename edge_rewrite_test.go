package sqlparser

import (
	"fmt"
	"testing"
)

const shopAuthorOriginalSQL = `SELECT  point1_id,
        point2_id,
        point1_type,
        point2_type,
        value,
        ts_us,
        edge_type,
        cast(order_rate_weight as float)
FROM    (
            SELECT  distinct src AS point1_id,
                    tgt AS point2_id,
                    'shop' AS point1_type,
                    'author' AS point2_type,
                    '' AS value,
                    (UNIX_TIMESTAMP() * 1000000) AS ts_us,
                    'shop_author' AS edge_type,
                    ratio_src AS order_rate_weight
            FROM    dm_temai.shop_gandalf_v1_3_graph_structure_di
            WHERE   date = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di')
            and edge_type = 'shop_sell_author_1d'
        ) a`

const authorShopOriginalSQL = `SELECT  DISTINCT point1_id,
        point2_id,
        point1_type,
        point2_type,
        value,
        ts_us,
        edge_type,
        cast(order_rate_weight as float)
FROM    (
            SELECT  src AS point2_id,
                    tgt AS point1_id,
                    'shop' AS point2_type,
                    'author' AS point1_type,
                    '' AS value,
                    (UNIX_TIMESTAMP() * 1000000) AS ts_us,
                    'author_shop' AS edge_type,
                    ratio_src AS order_rate_weight
            FROM    dm_temai.shop_gandalf_v1_3_graph_structure_di
            WHERE   date = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di')
            and edge_type = 'shop_sell_author_1d'
        ) a`

func TestRewriteShopAuthorEdgeQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		input          string
		wantNormalized string
		wantRewritten  string
	}{{
		name:           "shop_to_author",
		input:          shopAuthorOriginalSQL,
		wantNormalized: "select point1_id, point2_id, point1_type, point2_type, value, ts_us, edge_type, convert(order_rate_weight, float) from (select distinct src as point1_id, tgt as point2_id, 'shop' as point1_type, 'author' as point2_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'shop_author' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_author_1d') as a",
		wantRewritten:  "select named_struct('id', convert(point1_id, string)) as outv_pk_prop, convert(point2_id, string) as bg__id, point1_type as outv_label, point2_type as bg__bg__label, edge_type as label, convert(order_rate_weight, float) as order_rate_weight from (select distinct src as point1_id, tgt as point2_id, 'shop' as point1_type, 'author' as point2_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'shop_author' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_author_1d') as a",
	}, {
		name:           "author_to_shop",
		input:          authorShopOriginalSQL,
		wantNormalized: "select distinct point1_id, point2_id, point1_type, point2_type, value, ts_us, edge_type, convert(order_rate_weight, float) from (select src as point2_id, tgt as point1_id, 'shop' as point2_type, 'author' as point1_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'author_shop' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_author_1d') as a",
		wantRewritten:  "select distinct named_struct('id', convert(point1_id, string)) as outv_pk_prop, convert(point2_id, string) as bg__id, point1_type as outv_label, point2_type as bg__bg__label, edge_type as label, convert(order_rate_weight, float) as order_rate_weight from (select src as point2_id, tgt as point1_id, 'shop' as point2_type, 'author' as point1_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'author_shop' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_author_1d') as a",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%s) error: %v", tt.name, err)
			}

			if got := String(stmt); got != tt.wantNormalized {
				t.Fatalf("normalized SQL mismatch\nwant: %s\n got: %s", tt.wantNormalized, got)
			}

			sel, ok := stmt.(*Select)
			if !ok {
				t.Fatalf("expected *Select, got %T", stmt)
			}

			rewritePointEdgeSelect(t, sel)

			if got := String(sel); got != tt.wantRewritten {
				t.Fatalf("rewritten SQL mismatch\nwant: %s\n got: %s", tt.wantRewritten, got)
			}
		})
	}
}

func rewritePointEdgeSelect(t testing.TB, sel *Select) {
	t.Helper()

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

	if point1ID == nil || point2ID == nil || point1Type == nil || point2Type == nil {
		t.Fatalf("missing required columns: p1=%t p2=%t p1Type=%t p2Type=%t", point1ID != nil, point2ID != nil, point1Type != nil, point2Type != nil)
	}

	selectExprs := SelectExprs{
		newAliasedExprFromString(t, fmt.Sprintf("named_struct('id', cast(%s as string))", String(point1ID.Expr)), "outv_pk_prop"),
		newAliasedExprFromString(t, fmt.Sprintf("cast(%s as string)", String(point2ID.Expr)), "bg__id"),
	}

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

func newAliasedExprFromString(t testing.TB, expr, alias string) *AliasedExpr {
	t.Helper()
	parsedExpr := mustParseExpr(t, expr)
	return &AliasedExpr{
		Expr: parsedExpr,
		As:   NewColIdent(alias),
	}
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

func mustParseExpr(t testing.TB, expr string) Expr {
	t.Helper()
	stmt, err := Parse("select " + expr)
	if err != nil {
		t.Fatalf("Parse expression %q: %v", expr, err)
	}
	sel, ok := stmt.(*Select)
	if !ok || len(sel.SelectExprs) != 1 {
		t.Fatalf("unexpected expression parse tree for %q", expr)
	}
	aliased, ok := sel.SelectExprs[0].(*AliasedExpr)
	if !ok {
		t.Fatalf("expression %q did not parse as *AliasedExpr", expr)
	}
	return aliased.Expr
}
