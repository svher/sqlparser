package sqlparser

import (
	"encoding/json"
	"testing"
)

func TestRewriteEdgeSqls(t *testing.T) {
	typeMap := map[string]map[string]string{
		"shop_sim": {
			"order_rate_weight": "float",
		},
		"sim_author": {
			"order_rate_weight": "double",
			"property2":         "double",
		},
	}
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
        cast(order_rate_weight as double),
        property2
FROM    (
        SELECT  src AS point2_id,
                tgt AS point1_id,
                'author' AS point2_type,
                'sim' AS point1_type,
                '' AS value,
                (UNIX_TIMESTAMP() * 1000000) AS ts_us,
                'sim_author' AS edge_type,
                ratio_src AS order_rate_weight,
                'prop' AS property2
        FROM    dm_temai.shop_gandalf_v1_3_graph_structure_di
        WHERE   date = max_pt('dm_temai.shop_gandalf_v1_3_graph_structure_di')
        AND     edge_type = 'author_sell_sim_1d'
        ) a;`, false, typeMap)
	if err != nil {
		t.Fatalf("RewriteSqls error: %v", err)
	}
	if len(rewritten) != 2 {
		t.Fatalf("expected 2 rewritten sqls, got %d", len(rewritten))
	}

	buffer, _ := json.MarshalIndent(rewritten, "", "  ")
	t.Log("\n" + string(buffer))
}

func TestRewritePointSql(t *testing.T) {
	rewritten, err := RewriteSqls(`with t1 as (select * from t)
SELECT
    concat(cast(group_id as string), '_', main_vertical_category_ka_new) as point_id,
  'group' as point_type,
  concat(cast(group_id as string), '_', main_vertical_category_ka_new) as point_value,
  cast(group_size as double),
  cast(total_group_size as double),
  final_score_five,
  cast(self_dep_shop_cnt as double),
  cast(self_com_shop_cnt as double),
  cast(low_level_shop_cnt as double),
  cast(close_cnt as double),
  cast(deport_cnt as double),
  close_ratio,
  deport_ratio,
  cast(product_ban_cnt as double),
  avg_product_ban_cnt,
  cast(weak_fake_pdc_30d as double),
  cast(strong_fake_pdc as double),
  avg_strong_fake_pdc,
  cast(strong_copy_pdc as double),
  cast(pay_order_cnt_7d as double),
  cast(pay_gmv_7d as double),
  cast(ontime_order_cnt_7d as double),
  cast(sale_refund_sucess_order_cnt_7d as double),
  bad_eval_ratio_td,
  cast(eval_cnt_td as double),
  cast(bad_eval_cnt_td as double),
  cast(good_eval_cnt_td as double),
  cast(bad_eval_cnt_7d as double),
  cast(bad_eval_ratio_7d as double),
  good_eval_ratio_td,
  good_eval_ratio_7d,
  quality_refund_order_rate_td,
  cast(quality_refund_order_cnt_7d as double),
  complaint_order_rate_7d,
  cast(complaint_cnt_7d as double),
  cast(machine_audit_reject_product_cnt_7d as double),
  cast(product_bad_eval_cnt as double),
  cast(audit_success_product_cnt as double),
  cast(create_product_cnt as double),
  cast(create_ucnt as double),
  cast(ccr_cnt as double),
  cast(dz_cnt as double),
  cast(jhsz_cnt as double),
  cast(jxs_cnt as double),
  cast(zwyl_cnt as double),
  p0_ccr_ratio_30d,
  cast(p0_ccr_cnt_30d as double),
  cast(pay_order_cnt_for_p0_ccr_30d as double),
  ccr_ratio_60d,
  cast(ccr_cnt_60d as double),
  cast(quality_ccr_cnt_60d as double),
  cast(order_cnt_for_ccr as double),
  quality_ccr_ratio_60d,
  content_ccr_ratio_60d,
  service_ccr_ratio_60d,
  p0_ccr_ratio_30d_pct_vet,
  ccr_ratio_60d_pct_vet,
  quality_ccr_ratio_60d_pct_vet,
  bad_eval_ratio_td_pct_vet,
  good_eval_ratio_td_pct_vet,
  severe_quality_return_rate,
  severe_quality_return_rate_diff,
  product_bad_eval_ratio_td,
  ontime_order_rate_td
FROM
  dm_temai.shop_fusion_group_aggregation_feature_by_vet
join t
on 1 = 1
WHERE
  date = max_pt('dm_temai.shop_fusion_group_aggregation_feature_by_vet') and 1 = 1 or (2 =2 and 3=3) group by d order by e`, false, nil)
	if err != nil {
		t.Fatalf("RewriteSqls error: %v", err)
	}
	if len(rewritten) != 1 {
		t.Fatalf("expected 1 rewritten sql, got %d", len(rewritten))
	}

	buffer, _ := json.MarshalIndent(rewritten, "", "  ")
	t.Log("\n" + string(buffer))
}
