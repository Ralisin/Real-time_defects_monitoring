package ralisin.it.maps;

import org.apache.flink.api.common.functions.MapFunction;
import ralisin.it.model.OutlierPoint;
import ralisin.it.model.OutlierResult;
import ralisin.it.model.ClusteredResult;
import ralisin.it.model.ClusteredResult.Centroid;
import smile.clustering.DBSCAN;

import java.util.ArrayList;
import java.util.List;

public class OutlierClustering implements MapFunction<OutlierResult, ClusteredResult> {
    private final double eps;
    private final int minSamples;

    public OutlierClustering(double eps, int minSamples) {
        this.eps = eps;
        this.minSamples = minSamples;
    }

    @Override
    public ClusteredResult map(OutlierResult value) throws Exception {
        int seqId = value.batchId;
        String printId = value.printId;
        int tileId = value.tileId;
        int saturatedCount = value.saturatedCount;
        List<OutlierPoint> outlierPoints = value.outlierPoints;

        if (outlierPoints == null || outlierPoints.isEmpty()) {
            return new ClusteredResult(seqId, printId, tileId, saturatedCount, new ArrayList<>());
        }

        // Convert OutlierPoints to double[][] for smile DBSCAN
        double[][] points = new double[outlierPoints.size()][2];
        for (int i = 0; i < outlierPoints.size(); i++) {
            OutlierPoint p = outlierPoints.get(i);
            points[i][0] = p.x;
            points[i][1] = p.y;
        }

        // Perform DBSCAN clustering
        DBSCAN<double[]> clustering = DBSCAN.fit(points, minSamples, eps);

        // Access cluster labels via 'y'
        int[] labels = clustering.y;

        // Extract unique clusters (exclude noise = Integer.MAX_VALUE)
        List<Integer> uniqueLabels = new ArrayList<>();
        for (int label : labels) {
            if (label != Integer.MAX_VALUE && !uniqueLabels.contains(label)) {
                uniqueLabels.add(label);
            }
        }

        // Compute centroids for each cluster
        List<Centroid> centroids = new ArrayList<>();
        for (int label : uniqueLabels) {
            double sumX = 0.0;
            double sumY = 0.0;
            int count = 0;
            for (int i = 0; i < labels.length; i++) {
                if (labels[i] == label) {
                    sumX += points[i][0];
                    sumY += points[i][1];
                    count++;
                }
            }
            centroids.add(new Centroid(sumX / count, sumY / count, count));
        }

        return new ClusteredResult(seqId, printId, tileId, saturatedCount, centroids);
    }
}
