package sqlparser

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

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

const multiOriginalSQL = `SELECT  DISTINCT point1_id,
        point2_id,
        point1_type,
        point2_type,
        value,
        ts_us,
        edge_type,
        cast(order_rate_weight as float)
FROM    (
        SELECT  src AS point1_id,
                tgt AS point2_id,
                'shop' AS point1_type,
                'sim' AS point2_type,
                '' AS value,
                (UNIX_TIMESTAMP() * 1000000) AS ts_us,
                'shop_sim' AS edge_type,
               ratio_src AS order_rate_weight
        FROM    dm_temai.shop_gandalf_v1_3_graph_structure_di
        WHERE   date = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di')
        AND     edge_type = 'shop_sell_sim_1d'
        ) a;

SELECT  DISTINCT point1_id,
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
                'sim' AS point1_type,
                '' AS value,
                (UNIX_TIMESTAMP() * 1000000) AS ts_us,
                'sim_shop' AS edge_type,
                ratio_src AS order_rate_weight
        FROM    dm_temai.shop_gandalf_v1_3_graph_structure_di
        WHERE   date = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di')
        AND     edge_type = 'shop_sell_sim_1d'
        ) a;

SELECT  DISTINCT point1_id,
        point2_id,
        point1_type,
        point2_type,
        value,
        ts_us,
        edge_type,
        cast(order_rate_weight as float)
FROM    (
        SELECT  src AS point1_id,
                tgt AS point2_id,
                'author' AS point1_type,
                'sim' AS point2_type,
                '' AS value,
                (UNIX_TIMESTAMP() * 1000000) AS ts_us,
                'author_sim' AS edge_type,
                ratio_src AS order_rate_weight
        FROM    dm_temai.shop_gandalf_v1_3_graph_structure_di
        WHERE   date = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di')
        AND     edge_type = 'author_sell_sim_1d'
        ) a;

SELECT  DISTINCT point1_id,
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
                'author' AS point2_type,
                'sim' AS point1_type,
                '' AS value,
                (UNIX_TIMESTAMP() * 1000000) AS ts_us,
                'sim_author' AS edge_type,
                ratio_src AS order_rate_weight
        FROM    dm_temai.shop_gandalf_v1_3_graph_structure_di
        WHERE   date = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di')
        AND     edge_type = 'author_sell_sim_1d'
        ) a;`

const multiRewrittenSQL = "select distinct named_struct('id', cast(point1_id as string)) as outv_pk_prop, cast(point2_id as string) as bg__id, point1_type as outv_label, point2_type as bg__bg__label, edge_type as label, cast(order_rate_weight as float) as order_rate_weight from (select src as point1_id, tgt as point2_id, 'shop' as point1_type, 'sim' as point2_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'shop_sim' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_sim_1d') as a;\nselect distinct named_struct('id', cast(point1_id as string)) as outv_pk_prop, cast(point2_id as string) as bg__id, point1_type as outv_label, point2_type as bg__bg__label, edge_type as label, cast(order_rate_weight as float) as order_rate_weight from (select src as point2_id, tgt as point1_id, 'shop' as point2_type, 'sim' as point1_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'sim_shop' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_sim_1d') as a;\nselect distinct named_struct('id', cast(point1_id as string)) as outv_pk_prop, cast(point2_id as string) as bg__id, point1_type as outv_label, point2_type as bg__bg__label, edge_type as label, cast(order_rate_weight as float) as order_rate_weight from (select src as point1_id, tgt as point2_id, 'author' as point1_type, 'sim' as point2_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'author_sim' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'author_sell_sim_1d') as a;\nselect distinct named_struct('id', cast(point1_id as string)) as outv_pk_prop, cast(point2_id as string) as bg__id, point1_type as outv_label, point2_type as bg__bg__label, edge_type as label, cast(order_rate_weight as float) as order_rate_weight from (select src as point2_id, tgt as point1_id, 'author' as point2_type, 'sim' as point1_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'sim_author' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'author_sell_sim_1d') as a"

const pointOriginalSQL = `SELECT
  sim_id as point_id,
  'sim' as point_type,
  sim_id as point_value,
  prod_cnt,
  second_cid_new,
  shop_cnt,
  create_prod_cnt_90d,
  update_prod_cnt_90d,
  dx_prod_cnt_his,
  banned_product_cnt_90d,
  banned_product_cnt_td,
  delete_product_cnt_90d,
  delete_product_cnt_td,
  banned_product_ratio_td,
  banned_product_ratio_90d,
  delete_product_ratio_td,
  delete_product_ratio_90d,
  prod_bad_eval_ratio_td,
  prod_bad_eval_ratio_30d,
  prod_good_eval_ratio_td,
  prod_good_eval_ratio_30d,
  product_quality_refund_order_ratio_td,
  product_quality_refund_order_ratio_30d,
  complain_compl_ratio_200d,
  live_click_show_raito_30d,
  live_d_o_ratio_30d,
  explain_ucnt_30d,
  live_ucnt_30d,
  ecqc_refuse_ratio,
  cast(ecqc_refuse_task_cnt as double),
  tcs_refuse_ratio,
  machine_audit_fail_ratio,
  machine_audit_fail_cnt,
  audit_fail_ratio,
  audit_fail_cnt,
  second_cid_conflict_cnt,
  second_cid_conflict_ratio,
  first_cid_conflict_cnt,
  first_cid_conflict_ratio,
  order_cnt,
  ccr_order_cnt,
  quality_ccr_order_cnt,
  content_ccr_order_cnt,
  service_ccr_order_cnt,
  p0_ccr_order_cnt,
  bad_comment_order_cnt,
  good_comment_order_cnt,
  quality_return_order_cnt,
  complaint_order_cnt,
  author_cnt,
  banned_product_cnt,
  delete_product_cnt,
  clear_shop_cnt,
  low_level_shop_cnt,
  low_score_shop_cnt,
  low_score_author_cnt,
  no_auth_author_cnt,
  low_level_author_cnt,
  comment_order_cnt,
  ccr_ratio,
  quality_ccr_ratio,
  content_ccr_ratio,
  service_ccr_ratio,
  p0_ccr_ratio,
  bad_comment_ratio,
  good_comment_ratio,
  quality_return_order_ratio,
  complaint_order_ratio,
  banned_product_ratio,
  delete_product_ratio,
  clear_shop_ratio,
  low_level_shop_ratio,
  low_score_shop_ratio,
  no_auth_author_ratio,
  low_score_author_ratio,
  low_level_author_ratio,
  ccr_ratio_pct_second,
  quality_ccr_ratio_pct_second,
  content_ccr_ratio_pct_second,
  service_ccr_ratio_pct_second,
  p0_ccr_ratio_pct_second,
  bad_comment_ratio_pct_second,
  good_comment_ratio_pct_second,
  quality_return_ratio_pct_second,
  complaint_ratio_pct_second,
  prod_set
FROM
  dm_temai.sim_product_feature_aggregation_new_df
WHERE
  date = max_pt('dm_temai.sim_product_feature_aggregation_new_df')`

const pointRewrittenSQL = "select 'sim' as label, cast(sim_id as string) as id, prod_cnt, second_cid_new, shop_cnt, create_prod_cnt_90d, update_prod_cnt_90d, dx_prod_cnt_his, banned_product_cnt_90d, banned_product_cnt_td, delete_product_cnt_90d, delete_product_cnt_td, banned_product_ratio_td, banned_product_ratio_90d, delete_product_ratio_td, delete_product_ratio_90d, prod_bad_eval_ratio_td, prod_bad_eval_ratio_30d, prod_good_eval_ratio_td, prod_good_eval_ratio_30d, product_quality_refund_order_ratio_td, product_quality_refund_order_ratio_30d, complain_compl_ratio_200d, live_click_show_raito_30d, live_d_o_ratio_30d, explain_ucnt_30d, live_ucnt_30d, ecqc_refuse_ratio, cast(ecqc_refuse_task_cnt as double), tcs_refuse_ratio, machine_audit_fail_ratio, machine_audit_fail_cnt, audit_fail_ratio, audit_fail_cnt, second_cid_conflict_cnt, second_cid_conflict_ratio, first_cid_conflict_cnt, first_cid_conflict_ratio, order_cnt, ccr_order_cnt, quality_ccr_order_cnt, content_ccr_order_cnt, service_ccr_order_cnt, p0_ccr_order_cnt, bad_comment_order_cnt, good_comment_order_cnt, quality_return_order_cnt, complaint_order_cnt, author_cnt, banned_product_cnt, delete_product_cnt, clear_shop_cnt, low_level_shop_cnt, low_score_shop_cnt, low_score_author_cnt, no_auth_author_cnt, low_level_author_cnt, comment_order_cnt, ccr_ratio, quality_ccr_ratio, content_ccr_ratio, service_ccr_ratio, p0_ccr_ratio, bad_comment_ratio, good_comment_ratio, quality_return_order_ratio, complaint_order_ratio, banned_product_ratio, delete_product_ratio, clear_shop_ratio, low_level_shop_ratio, low_score_shop_ratio, no_auth_author_ratio, low_score_author_ratio, low_level_author_ratio, ccr_ratio_pct_second, quality_ccr_ratio_pct_second, content_ccr_ratio_pct_second, service_ccr_ratio_pct_second, p0_ccr_ratio_pct_second, bad_comment_ratio_pct_second, good_comment_ratio_pct_second, quality_return_ratio_pct_second, complaint_ratio_pct_second, prod_set from dm_temai.sim_product_feature_aggregation_new_df where `date` = max_pt('dm_temai.sim_product_feature_aggregation_new_df')"

func TestRewriteShopAuthorEdgeQueries(t *testing.T) {
	t.Parallel()

	const shopToAuthorOriginalSQL = `SELECT  point1_id,
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

	tests := []struct {
		name           string
		input          string
		wantNormalized string
		wantRewritten  string
	}{{
		name:           "shop_to_author",
		input:          shopToAuthorOriginalSQL,
		wantNormalized: "select point1_id, point2_id, point1_type, point2_type, value, ts_us, edge_type, cast(order_rate_weight as float) from (select distinct src as point1_id, tgt as point2_id, 'shop' as point1_type, 'author' as point2_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'shop_author' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_author_1d') as a",
		wantRewritten:  "select named_struct('id', cast(point1_id as string)) as outv_pk_prop, cast(point2_id as string) as bg__id, point1_type as outv_label, point2_type as bg__bg__label, edge_type as label, cast(order_rate_weight as float) as order_rate_weight from (select distinct src as point1_id, tgt as point2_id, 'shop' as point1_type, 'author' as point2_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'shop_author' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_author_1d') as a",
	}, {
		name:           "author_to_shop",
		input:          authorShopOriginalSQL,
		wantNormalized: "select distinct point1_id, point2_id, point1_type, point2_type, value, ts_us, edge_type, cast(order_rate_weight as float) from (select src as point2_id, tgt as point1_id, 'shop' as point2_type, 'author' as point1_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'author_shop' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_author_1d') as a",
		wantRewritten:  "select distinct named_struct('id', cast(point1_id as string)) as outv_pk_prop, cast(point2_id as string) as bg__id, point1_type as outv_label, point2_type as bg__bg__label, edge_type as label, cast(order_rate_weight as float) as order_rate_weight from (select src as point2_id, tgt as point1_id, 'shop' as point2_type, 'author' as point1_type, '' as value, (UNIX_TIMESTAMP() * 1000000) as ts_us, 'author_shop' as edge_type, ratio_src as order_rate_weight from dm_temai.shop_gandalf_v1_3_graph_structure_di where `date` = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di') and edge_type = 'shop_sell_author_1d') as a",
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

func TestRewritePointEdgeMultiStatement(t *testing.T) {
	t.Parallel()

	got := rewritePointEdgeStatements(t, multiOriginalSQL)
	if got != multiRewrittenSQL {
		t.Fatalf("multi-statement rewrite mismatch\nwant: %s\n got: %s", multiRewrittenSQL, got)
	}
}

func TestRewritePointStatementHelper(t *testing.T) {
	t.Parallel()

	if got := rewritePointEdgeStatements(t, pointOriginalSQL); got != pointRewrittenSQL {
		t.Fatalf("point rewrite mismatch\nwant: %s\n got: %s", pointRewrittenSQL, got)
	}
}

func TestRewritePointQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		input          string
		wantNormalized string
		wantRewritten  string
	}{{
		name:           "sim_point",
		input:          pointOriginalSQL,
		wantNormalized: "select sim_id as point_id, 'sim' as point_type, sim_id as point_value, prod_cnt, second_cid_new, shop_cnt, create_prod_cnt_90d, update_prod_cnt_90d, dx_prod_cnt_his, banned_product_cnt_90d, banned_product_cnt_td, delete_product_cnt_90d, delete_product_cnt_td, banned_product_ratio_td, banned_product_ratio_90d, delete_product_ratio_td, delete_product_ratio_90d, prod_bad_eval_ratio_td, prod_bad_eval_ratio_30d, prod_good_eval_ratio_td, prod_good_eval_ratio_30d, product_quality_refund_order_ratio_td, product_quality_refund_order_ratio_30d, complain_compl_ratio_200d, live_click_show_raito_30d, live_d_o_ratio_30d, explain_ucnt_30d, live_ucnt_30d, ecqc_refuse_ratio, cast(ecqc_refuse_task_cnt as double), tcs_refuse_ratio, machine_audit_fail_ratio, machine_audit_fail_cnt, audit_fail_ratio, audit_fail_cnt, second_cid_conflict_cnt, second_cid_conflict_ratio, first_cid_conflict_cnt, first_cid_conflict_ratio, order_cnt, ccr_order_cnt, quality_ccr_order_cnt, content_ccr_order_cnt, service_ccr_order_cnt, p0_ccr_order_cnt, bad_comment_order_cnt, good_comment_order_cnt, quality_return_order_cnt, complaint_order_cnt, author_cnt, banned_product_cnt, delete_product_cnt, clear_shop_cnt, low_level_shop_cnt, low_score_shop_cnt, low_score_author_cnt, no_auth_author_cnt, low_level_author_cnt, comment_order_cnt, ccr_ratio, quality_ccr_ratio, content_ccr_ratio, service_ccr_ratio, p0_ccr_ratio, bad_comment_ratio, good_comment_ratio, quality_return_order_ratio, complaint_order_ratio, banned_product_ratio, delete_product_ratio, clear_shop_ratio, low_level_shop_ratio, low_score_shop_ratio, no_auth_author_ratio, low_score_author_ratio, low_level_author_ratio, ccr_ratio_pct_second, quality_ccr_ratio_pct_second, content_ccr_ratio_pct_second, service_ccr_ratio_pct_second, p0_ccr_ratio_pct_second, bad_comment_ratio_pct_second, good_comment_ratio_pct_second, quality_return_ratio_pct_second, complaint_ratio_pct_second, prod_set from dm_temai.sim_product_feature_aggregation_new_df where `date` = max_pt('dm_temai.sim_product_feature_aggregation_new_df')",
		wantRewritten:  pointRewrittenSQL,
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

func rewritePointEdgeStatements(t testing.TB, sql string) string {
	t.Helper()

	tokenizer := NewStringTokenizer(sql)
	var rewritten []string
	for {
		stmt, err := ParseNext(tokenizer)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ParseNext error: %v", err)
		}
		if stmt == nil {
			continue
		}
		selectStmt, ok := stmt.(*Select)
		if !ok {
			t.Fatalf("unexpected statement type %T", stmt)
		}
		rewritePointEdgeSelect(t, selectStmt)
		rewritten = append(rewritten, String(selectStmt))
	}

	return strings.Join(rewritten, ";\n")
}
func rewritePointEdgeSelect(t testing.TB, sel *Select) {
	t.Helper()

	if rewriteEdgeSelect(t, sel) {
		return
	}
	if rewritePointSelect(t, sel) {
		return
	}

	t.Fatalf("select does not contain recognizable point or edge columns")
}

func rewriteEdgeSelect(t testing.TB, sel *Select) bool {
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

	if point1ID == nil && point2ID == nil && point1Type == nil && point2Type == nil {
		return false
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
	return true
}

func rewritePointSelect(t testing.TB, sel *Select) bool {
	t.Helper()

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
		return false
	}

	if pointID == nil || pointType == nil {
		t.Fatalf("missing required point columns: point_id=%t point_type=%t", pointID != nil, pointType != nil)
	}

	pointType.As = NewColIdent("label")

	selectExprs := SelectExprs{pointType}
	selectExprs = append(selectExprs, newAliasedExprFromString(t, fmt.Sprintf("cast(%s as string)", String(pointID.Expr)), "id"))

	for _, expr := range remaining {
		selectExprs = append(selectExprs, expr)
	}

	sel.SelectExprs = selectExprs
	return true
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
