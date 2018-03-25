package task3;

import org.apache.flink.api.java.tuple.Tuple5;

public class Centroid extends Point {
	
	public int id;
	public long cnt;
	
	public Centroid(){
		
	}
	
	public Centroid(int id, double x, double y, double z) {
		super(x,y,z);
		this.id = id;
	}
	
	public Centroid(int id, long cnt, Point p) {
		super(p.x, p.y , p.z);
		this.id = id;
		this.cnt = cnt;
	}
	
	public Centroid(int id, Point p) {
		super(p.x, p.y , p.z);
		this.id = id;
	}
	
	public Point getPoint(){
		return new Point(x,y,z);
	}
	
	@Override
	public String toString() {
		return id + " " + cnt + " " + super.toString();
	}

}
