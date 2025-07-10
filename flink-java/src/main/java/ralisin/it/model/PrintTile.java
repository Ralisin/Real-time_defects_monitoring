package ralisin.it.model;

public class PrintTile {
    public String print_id;
    public int batch_id;
    public int tile_id;
    public int layer;
    public byte[] tif;

    public PrintTile(String print_id, int batch_id, int tile_id, int layer, byte[] tif) {
        this.print_id = print_id;
        this.batch_id = batch_id;
        this.tile_id = tile_id;
        this.layer = layer;
        this.tif = tif;
    }

    @Override
    public String toString() {
        return "PrintTile{" +
                "print_id='" + print_id + '\'' +
                ", batch_id='" + batch_id + '\'' +
                ", tile_id='" + tile_id + '\'' +
                ", layer=" + layer +
                ", tif=" + (tif != null ? ("[bytes:" + tif.length + "]") : "null") +
                '}';
    }
}