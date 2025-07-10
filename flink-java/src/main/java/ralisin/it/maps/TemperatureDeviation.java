package ralisin.it.maps;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import ralisin.it.model.PrintTileQ1;
import ralisin.it.model.OutlierResult;
import ralisin.it.model.OutlierPoint;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.util.*;

public class TemperatureDeviation extends ProcessWindowFunction<PrintTileQ1, OutlierResult, Integer, GlobalWindow> {
    private static final float DEVIATION_THRESHOLD = 6000f;

    @Override
    public void process(Integer key, Context context, Iterable<PrintTileQ1> elements, Collector<OutlierResult> out) throws Exception {
        try {
            List<PrintTileQ1> events = new ArrayList<>();
            elements.forEach(events::add);
            events.sort(Comparator.comparingInt(e -> e.layer));
            if (events.size() != 3) return;

            PrintTileQ1 top = events.get(2);
            List<Integer> layers = Arrays.asList(events.get(0).layer, events.get(1).layer, events.get(2).layer);

            // Load images: [3][h][w]
            float[][][] image3d = new float[3][][];
            int height = -1, width = -1;
            for (int i = 0; i < 3; i++) {
                image3d[i] = tiffToFloatArray(events.get(i).tif);
                if (height == -1) {
                    height = image3d[i].length;
                    width = image3d[i][0].length;
                }
            }

            List<OutlierPoint> outlierPoints = new ArrayList<>();
            // Loop every pixel from top layer
            for (int x = 0; x < height; x++) {
                for (int y = 0; y < width; y++) {
                    // Get 3x5x5 window centered on (x,y), handling edges
                    List<Float> localNeighbors = new ArrayList<>();
                    List<Float> externalNeighbors = new ArrayList<>();
                    for (int l = 0; l < 3; l++) {
                        for (int i = -2; i <= 2; i++) {
                            for (int j = -2; j <= 2; j++) {
                                int xx = x + i;
                                int yy = y + j;

                                if (xx < 0 || xx >= height || yy < 0 || yy >= width)
                                    continue; // Simulates padding with NaN (skips out of image)

                                float v = image3d[l][xx][yy];
                                int dist = Math.abs(l - 2) + Math.abs(i) + Math.abs(j);
                                if (dist <= 2) localNeighbors.add(v);
                                else if (dist <= 4) externalNeighbors.add(v);
                            }
                        }
                    }

                    float avgLocal = nanmean(localNeighbors);
                    float avgExternal = nanmean(externalNeighbors);

                    if (Float.isNaN(avgLocal) || Float.isNaN(avgExternal)) continue;

                    float deviation = Math.abs(avgLocal - avgExternal);
                    if (deviation > DEVIATION_THRESHOLD) {
                        outlierPoints.add(new OutlierPoint(x, y, deviation));
                    }
                }
            }

            out.collect(new OutlierResult(
                top.print_id, top.batch_id, top.tile_id, layers, top.saturated_count, outlierPoints
            ));
        } catch (Exception e) {
            System.err.printf("[TemperatureDeviation] Error: %s%n", e);
            out.collect(new OutlierResult("", -1, -1, Collections.emptyList(), -1, Collections.emptyList()));
        }
    }

    // --- Utility: TIFF to float[][]
    private float[][] tiffToFloatArray(byte[] tiffBytes) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(tiffBytes);
        BufferedImage img = ImageIO.read(bais);
        int h = img.getHeight(), w = img.getWidth();
        float[][] arr = new float[h][w];
        for (int y = 0; y < h; y++)
            for (int x = 0; x < w; x++)
                arr[y][x] = img.getRaster().getSample(x, y, 0);
        return arr;
    }

    // --- Utility: nanmean
    private float nanmean(List<Float> vals) {
        float sum = 0; int count = 0;
        for (float v : vals) if (!Float.isNaN(v)) { sum += v; count++; }
        return count == 0 ? Float.NaN : sum / count;
    }
}