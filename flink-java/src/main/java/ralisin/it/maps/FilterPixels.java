package ralisin.it.maps;

import org.apache.flink.api.common.functions.MapFunction;
import ralisin.it.model.PrintTile;
import ralisin.it.model.PrintTileQ1;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class FilterPixels implements MapFunction<PrintTile, PrintTileQ1> {
    private static final int SATURATION_THRESH = 60000;
    private static final int EMPTY_THRESH = 5000;

    @Override
    public PrintTileQ1 map(PrintTile tile) {
        try {
            BufferedImage image = ImageIO.read(new ByteArrayInputStream(tile.tif));
            if (image == null) throw new RuntimeException("Could not read TIFF image");

            int width = image.getWidth();
            int height = image.getHeight();

            int saturatedCount = 0;
            int[][] pixels = new int[height][width];

            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    int pix = image.getRaster().getSample(x, y, 0);
                    if (pix > SATURATION_THRESH) saturatedCount++;
                    boolean inRange = (pix >= EMPTY_THRESH) && (pix <= SATURATION_THRESH);
                    pixels[y][x] = inRange ? pix : 0;
                }
            }

            BufferedImage filteredImage = new BufferedImage(width, height, image.getType());
            for (int y = 0; y < height; y++)
                for (int x = 0; x < width; x++)
                    filteredImage.getRaster().setSample(x, y, 0, pixels[y][x]);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(filteredImage, "TIFF", baos);
            byte[] filteredBytes = baos.toByteArray();

            return new PrintTileQ1(
                    tile.print_id,
                    tile.batch_id,
                    tile.tile_id,
                    tile.layer,
                    saturatedCount,
                    filteredBytes
            );
        } catch (Exception e) {
            System.err.println("Error processing image: " + e);
            return new PrintTileQ1(
                    tile.print_id,
                    tile.batch_id,
                    tile.tile_id,
                    tile.layer,
                    0,
                    tile.tif
            );
        }
    }
}
