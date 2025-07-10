package ralisin.it.maps;

import org.apache.flink.api.common.functions.MapFunction;
import ralisin.it.model.OutlierPoint;
import ralisin.it.model.OutlierRank;
import ralisin.it.model.OutlierResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class OutlierRanker implements MapFunction<OutlierResult, OutlierRank> {

    @Override
    public OutlierRank map(OutlierResult row) throws Exception {
        try {
            // Ordina i punti outlier per deviazione decrescente
            List<OutlierPoint> sortedPoints = new ArrayList<>(row.outlierPoints);
            sortedPoints.sort(Comparator.comparingDouble(p -> -p.deviation));

            // Prendi i primi 5, riempi con valori default se meno di 5
            List<OutlierPoint> top5 = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                if (i < sortedPoints.size()) {
                    top5.add(sortedPoints.get(i));
                } else {
                    top5.add(new OutlierPoint(-1, -1, -1.0f));
                }
            }

            return new OutlierRank(
                    row.batchId,
                    row.printId,
                    row.tileId,
                    top5.get(0).x, top5.get(0).y, top5.get(0).deviation,
                    top5.get(1).x, top5.get(1).y, top5.get(1).deviation,
                    top5.get(2).x, top5.get(2).y, top5.get(2).deviation,
                    top5.get(3).x, top5.get(3).y, top5.get(3).deviation,
                    top5.get(4).x, top5.get(4).y, top5.get(4).deviation
            );

        } catch (Exception e) {
            System.err.println("[OutlierRanker] Error processing image: " + e);
            return new OutlierRank(
                    row.batchId,
                    row.printId,
                    row.tileId,
                    -2, -2, -2.0f,
                    -2, -2, -2.0f,
                    -2, -2, -2.0f,
                    -2, -2, -2.0f,
                    -2, -2, -2.0f
            );
        }
    }
}
