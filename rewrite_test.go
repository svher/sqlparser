package sqlparser

import (
	"encoding/json"
	"testing"
)

func TestRewriteEdgeSqls(t *testing.T) {
	typeMap := map[string]map[string]string{
		"shop_sim": {
			"order_rate_weight": "double",
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
        cast(order_rate_weight as float),
		low_score_author_cnt
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
	rewritten, err := RewriteSqls(`SELECT
concat(leaf_cid_new, '-', price_range_in) AS point_id,
        'leaf' AS point_type,
        concat(leaf_cid_new, '-', price_range_in) AS point_value,
  concat(leaf_cid_new, '-', price_range_in) as leaf_price,
  price_range_in,
  price_ranges[0] AS price_1_pt,
  price_ranges[1] AS price_5_pt,
  price_ranges[2] AS price_10_pt,
  price_ranges[3] AS price_25_pt,
  price_ranges[4] AS price_50_pt,
  price_ranges[5] AS price_75_pt,
  price_ranges[6] AS price_90_pt,
  price_ranges[7] AS price_95_pt,
  price_ranges[8] AS price_99_pt,
  cast(order_cnt as double),
  cast(ccr_order_cnt as double),
  cast(quality_ccr_order_cnt as double),
  cast(content_ccr_order_cnt as double),
  cast(service_ccr_order_cnt as double),
  cast(p0_ccr_order_cnt as double),
  cast(bad_comment_order_cnt as double),
  cast(good_comment_order_cnt as double),
  cast(quality_return_order_cnt as double),
  cast(complaint_order_cnt as double),
  cast(shop_cnt as double),
  cast(author_cnt as double),
  cast(product_cnt as double),
  cast(banned_product_cnt as double),
  cast(delete_product_cnt as double),
  cast(clear_shop_cnt as double),
  cast(low_level_shop_cnt as double),
  cast(low_score_shop_cnt as double),
  cast(low_score_author_cnt as double),
  cast(no_auth_author_cnt as double),
  cast(low_level_author_cnt as double),
  cast(comment_order_cnt as double),
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
  low_level_author_ratio
FROM
  dm_temai.shop_price_range_ccr_negative_new_30d
WHERE date = max_pt('dm_temai.shop_price_range_ccr_negative_new_30d')`, false, map[string]map[string]string{
		"group": {
			"quality_ccr_cnt_60d": "float",
		},
	})
	if err != nil {
		t.Fatalf("RewriteSqls error: %v", err)
	}
	if len(rewritten) != 1 {
		t.Fatalf("expected 1 rewritten sql, got %d", len(rewritten))
	}

	buffer, _ := json.MarshalIndent(rewritten, "", "  ")
	t.Log("\n" + string(buffer))
}
