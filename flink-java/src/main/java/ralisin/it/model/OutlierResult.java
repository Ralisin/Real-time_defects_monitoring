package ralisin.it.model;

import java.util.List;

public class OutlierResult {
    public String printId;
    public int batchId;
    public int tileId;
    public List<Integer> layers;
    public int saturatedCount;
    public List<OutlierPoint> outlierPoints;

    public OutlierResult(String printId, int batchId, int tileId, List<Integer> layers, int saturatedCount, List<OutlierPoint> outlierPoints) {
        this.printId = printId;
        this.batchId = batchId;
        this.tileId = tileId;
        this.layers = layers;
        this.saturatedCount = saturatedCount;
        this.outlierPoints = outlierPoints;
    }
}

