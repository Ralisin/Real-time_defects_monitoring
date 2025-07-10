package ralisin.it.model;

import java.util.List;

public class ClusteredResult {
    public int seqId;
    public String printId;
    public int tileId;
    public int saturatedCount;
    public List<Centroid> centroids;

    public ClusteredResult(int seqId, String printId, int tileId, int saturatedCount, List<Centroid> centroids) {
        this.seqId = seqId;
        this.printId = printId;
        this.tileId = tileId;
        this.saturatedCount = saturatedCount;
        this.centroids = centroids;
    }

    public static class Centroid {
        public double x;
        public double y;
        public int count;

        public Centroid(double x, double y, int count) {
            this.x = x;
            this.y = y;
            this.count = count;
        }
    }
}
