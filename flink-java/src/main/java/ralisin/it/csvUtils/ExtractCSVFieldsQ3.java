package ralisin.it.csvUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import ralisin.it.model.ClusteredResult;
import ralisin.it.model.ClusteredResult.Centroid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtractCSVFieldsQ3 implements MapFunction<ClusteredResult, String> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String map(ClusteredResult row) {
        List<Map<String, Object>> centroidsJsonList = getMaps(row);

        String centroidsJson;
        try {
            centroidsJson = mapper.writeValueAsString(centroidsJsonList);
        } catch (JsonProcessingException e) {
            centroidsJson = "[]";
        }

        System.out.printf("[ExtractCSVFieldsQ3] %d,%s,%d,%d,%s%n",
            row.seqId,
            row.printId,
            row.tileId,
            row.saturatedCount,
            centroidsJson
        );

        return String.format("%d,%s,%d,%d,%s",
                row.seqId,
                row.printId,
                row.tileId,
                row.saturatedCount,
                centroidsJson
        );
    }

    private static List<Map<String, Object>> getMaps(ClusteredResult row) {
        List<Map<String, Object>> centroidsJsonList = new ArrayList<>();

        for (Centroid c : row.centroids) {
            Map<String, Object> centroidMap = new HashMap<>();
            if (c == null || (c.x == -1 && c.y == -1 && c.count == -1)) {
                centroidMap.put("x", -1.0);
                centroidMap.put("y", -1.0);
                centroidMap.put("count", -1);
            } else {
                centroidMap.put("x", c.x);
                centroidMap.put("y", c.y);
                centroidMap.put("count", c.count);
            }
            centroidsJsonList.add(centroidMap);
        }
        return centroidsJsonList;
    }
}
