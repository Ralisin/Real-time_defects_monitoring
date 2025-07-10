package ralisin.it.csvUtils;

import org.apache.flink.api.common.functions.MapFunction;
import ralisin.it.model.OutlierRank;

public class ExtractCSVFieldsQ2 implements MapFunction<OutlierRank, String> {

    private String pointToStr(int x, int y) {
        if (x == -1 && y == -1) {
            return "(-1;-1)";
        } else {
            return "(" + x + ";" + y + ")";
        }
    }

    @Override
    public String map(OutlierRank row) {
        return row.seq_id + "," + row.print_id + "," + row.tile_id + "," +
                pointToStr(row.p1_x, row.p1_y) + "," + row.dp1 + "," +
                pointToStr(row.p2_x, row.p2_y) + "," + row.dp2 + "," +
                pointToStr(row.p3_x, row.p3_y) + "," + row.dp3 + "," +
                pointToStr(row.p4_x, row.p4_y) + "," + row.dp4 + "," +
                pointToStr(row.p5_x, row.p5_y) + "," + row.dp5;
    }
}
