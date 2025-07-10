package ralisin.it.csvUtils;

import org.apache.flink.api.common.functions.MapFunction;
import ralisin.it.model.PrintTileQ1;

public class ExtractCSVFieldsQ1 implements MapFunction<PrintTileQ1, String> {

    @Override
    public String map(PrintTileQ1 row) {
        return String.format("%d,%s,%d,%d",
                row.batch_id,
                row.print_id,
                row.tile_id,
                row.saturated_count
        );
    }
}
