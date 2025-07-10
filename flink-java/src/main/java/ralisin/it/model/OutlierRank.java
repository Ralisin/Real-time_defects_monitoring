package ralisin.it.model;

public class OutlierRank {
    public int seq_id;
    public String print_id;
    public int tile_id;

    public int p1_x, p1_y;
    public float dp1;
    public int p2_x, p2_y;
    public float dp2;
    public int p3_x, p3_y;
    public float dp3;
    public int p4_x, p4_y;
    public float dp4;
    public int p5_x, p5_y;
    public float dp5;

    public OutlierRank(int seq_id, String print_id, int tile_id,
                       int p1_x, int p1_y, float dp1,
                       int p2_x, int p2_y, float dp2,
                       int p3_x, int p3_y, float dp3,
                       int p4_x, int p4_y, float dp4,
                       int p5_x, int p5_y, float dp5) {
        this.seq_id = seq_id;
        this.print_id = print_id;
        this.tile_id = tile_id;
        this.p1_x = p1_x; this.p1_y = p1_y; this.dp1 = dp1;
        this.p2_x = p2_x; this.p2_y = p2_y; this.dp2 = dp2;
        this.p3_x = p3_x; this.p3_y = p3_y; this.dp3 = dp3;
        this.p4_x = p4_x; this.p4_y = p4_y; this.dp4 = dp4;
        this.p5_x = p5_x; this.p5_y = p5_y; this.dp5 = dp5;
    }

    @Override
    public String toString() {
        return String.format(
                "%d,%s,%d,(%d,%d),%.2f,(%d,%d),%.2f,(%d,%d),%.2f,(%d,%d),%.2f,(%d,%d),%.2f",
                seq_id, print_id, tile_id,
                p1_x, p1_y, dp1,
                p2_x, p2_y, dp2,
                p3_x, p3_y, dp3,
                p4_x, p4_y, dp4,
                p5_x, p5_y, dp5
        );
    }
}
