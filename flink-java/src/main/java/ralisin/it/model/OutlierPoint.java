package ralisin.it.model;

public class OutlierPoint {
    public int x;
    public int y;
    public float deviation;

    public OutlierPoint(int x, int y, float deviation) {
        this.x = x;
        this.y = y;
        this.deviation = deviation;
    }
}
