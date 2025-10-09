package sqlparser

import "testing"

func TestRewriteEdgeSqls(t *testing.T) {
	rewritten, err := RewriteSqls(`SELECT  DISTINCT point1_id,
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
                'author' AS point2_type,
                'sim' AS point1_type,
                '' AS value,
                (UNIX_TIMESTAMP() * 1000000) AS ts_us,
                'sim_author' AS edge_type,
                ratio_src AS order_rate_weight
        FROM    dm_temai.shop_gandalf_v1_3_graph_structure_di
        WHERE   date = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di')
        AND     edge_type = 'author_sell_sim_1d'
        ) a;`)
	if err != nil {
		t.Fatalf("RewriteSqls error: %v", err)
	}
	t.Log(rewritten)
}

func TestRewritePointSql(t *testing.T) {
	rewritten, err := RewriteSqls(`SELECT
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
  date = max_pt('dm_temai.sim_product_feature_aggregation_new_df')`)
	if err != nil {
		t.Fatalf("RewriteSqls error: %v", err)
	}
	t.Log(rewritten)
}
