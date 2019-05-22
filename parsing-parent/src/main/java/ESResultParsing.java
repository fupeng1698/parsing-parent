import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import org.junit.Test;
import pojo.Page;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 共有的es解析，包括解析es聚合和es查询结果
 */

public class ESResultParsing implements Parsing {



    private  List<Map<String, Object>> itemList = new ArrayList<Map<String, Object>>();


    @Test
    public void test() {

        String result = "{\n" +
                "  \"took\" : 1,\n" +
                "  \"timed_out\" : false,\n" +
                "  \"_shards\" : {\n" +
                "    \"total\" : 1,\n" +
                "    \"successful\" : 1,\n" +
                "    \"skipped\" : 0,\n" +
                "    \"failed\" : 0\n" +
                "  },\n" +
                "  \"hits\" : {\n" +
                "    \"total\" : 1,\n" +
                "    \"max_score\" : 0.0,\n" +
                "    \"hits\" : [ ]\n" +
                "  },\n" +
                "  \"aggregations\" : {\n" +
                "    \"fdbid\" : {\n" +
                "      \"doc_count_error_upper_bound\" : 0,\n" +
                "      \"sum_other_doc_count\" : 0,\n" +
                "      \"buckets\" : [\n" +
                "        {\n" +
                "          \"key\" : 1016,\n" +
                "          \"doc_count\" : 1,\n" +
                "          \"fid\" : {\n" +
                "            \"doc_count_error_upper_bound\" : 0,\n" +
                "            \"sum_other_doc_count\" : 0,\n" +
                "            \"buckets\" : [\n" +
                "              {\n" +
                "                \"key\" : 48470818,\n" +
                "                \"doc_count\" : 1,\n" +
                "                \"total\" : {\n" +
                "                  \"value\" : 1016.0\n" +
                "                },\n" +
                "                \"SUM(fid)\" : {\n" +
                "                  \"value\" : 4.8470818E7\n" +
                "                }\n" +
                "              }\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        System.out.println(parsing(result));

    }


    public  Page<Map<String, Object>> parsing(String result) {

        Page<Map<String,Object>> page = new Page<Map<String,Object>>();

        JSONObject resultObj = JSON.parseObject(result);

        JSONObject hitsObj = resultObj.getJSONObject("hits");

        JSONArray hits = hitsObj.getJSONArray("hits");

        if (hits.size() > 0) {
            List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();

            page.setItems(items);

            for (int i = 0; i < hits.size(); i++) {
                items.add(hits.getJSONObject(i));
            }
            page.setTotalCount(hitsObj.getLong("total"));

            return page;
        }

        JSONObject aggregations = resultObj.getJSONObject("aggregations");

        parsingAggregations(aggregations, null);

        page.setTotalCount(itemList.size());

        page.setItems(itemList);

        return page;
    }



    public  void parsingAggregations(JSONObject rootEntry, Map<String, Object> cycleMap) {

        if (rootEntry.containsKey("buckets")) {
            JSONArray buckets = rootEntry.getJSONArray("buckets");
            for (int i = 0; i < buckets.size(); i++) {
                JSONObject bucket = buckets.getJSONObject(i);

                boolean tag = false;

                for (String key : bucket.keySet()) {
                    if (!key.equals("key") && !key.equals("doc_count") && !key.equals("key_as_string")) {
                        // 是显示行
                        if (bucket.getJSONObject(key).containsKey("value")) {
                            tag = true;
                        }
                    }
                }
                Object value = bucket.get("key");
                for (Map.Entry<String,Object> entry : cycleMap.entrySet()) {
                    if (entry.getValue() == null) {
                        entry.setValue(value);
                    }
                }
                if (tag) {
                    Map<String, Object> itemMap = new HashMap<String, Object>();
                    for (String key : bucket.keySet()) {

                        if (!key.equals("key") && !key.equals("doc_count")) {
                            if (bucket.getJSONObject(key).containsKey("buckets")) {
                                ArrayList<Map<String, Object>> includeList = new ArrayList<Map<String, Object>>();
                                //处理下层集合问题
                                JSONArray includeArray = bucket.getJSONObject(key).getJSONArray("buckets");
                                for (int j = 0; j < includeArray.size(); j++) {
                                    Map<String, Object> includeMap = new HashMap<String, Object>();
                                    JSONObject includeObj = includeArray.getJSONObject(j);
                                    for (String includeKey : includeObj.keySet()) {
                                        if (includeKey.equals("key") || includeKey.equals("doc_count") || includeKey.equals("key_as_string")) {
                                            includeMap.put(includeKey, includeObj.get(includeKey));
                                        } else {
                                            Object includeValue = includeObj.getJSONObject(includeKey).get("value");
                                            includeMap.put(includeKey, includeValue);
                                        }
                                    }
                                    includeList.add(includeMap);
                                }
                                itemMap.put(key, includeList);
                            } else {
                                //下层非集合
                                Object valueObj = bucket.getJSONObject(key).get("value");
                                itemMap.put(key, valueObj);
                            }
                        }
                    }
                    if (cycleMap != null) {
                        for (Map.Entry<String,Object> entry : cycleMap.entrySet()) {
                            itemMap.put(entry.getKey(), entry.getValue());
                        }
                    }
                    itemList.add(itemMap);
                } else {

                    for (String key : bucket.keySet()) {
                        if (!key.equals("key") && !key.equals("doc_count") && !key.equals("key_as_string")) {
                            JSONObject nextObj = bucket.getJSONObject(key);
                            cycleMap.put(key, null);
                            parsingAggregations(nextObj, cycleMap);
                        }
                    }
                }
            }
        }
        else {
            for (String key : rootEntry.keySet()) {
                if (!key.equals("key") && !key.equals("doc_count") && !key.equals("key_as_string")) {
                    JSONObject nextObj = rootEntry.getJSONObject(key);
                    Object keyValue = nextObj.get("key");
                    if (cycleMap == null) {
                        cycleMap  = new HashMap<>();
                    }
                    cycleMap.put(key,keyValue);
                    parsingAggregations(nextObj, cycleMap);
                }
            }
        }
    }
}
