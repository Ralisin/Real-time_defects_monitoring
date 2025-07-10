package ralisin.it.model;

public class PrintTileQ1 extends PrintTile {
    private final int saturatedCount;
    public int saturated_count;

    public PrintTileQ1(String printId, int batchId, int tileId, int layer, int saturatedCount, byte[] tif) {
        super(printId, batchId, tileId, layer, tif);
        this.saturatedCount = saturatedCount;
    }

    public int getSaturatedCount() { return saturatedCount; }

    @Override
    public String toString() {
        return String.format("PrintTileQ1{print_id='%s', batch_id=%d, tile_id=%d, layer=%d, saturated_count=%d, tif=[bytes:%d]}",
                this.print_id, this.batch_id, this.tile_id, this.layer, saturatedCount, tif != null ? tif.length : 0);
    }
}
