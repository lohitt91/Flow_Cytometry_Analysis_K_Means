package task2;

public class Centroid extends Point{
	
	public int id;
	public Integer count = 0;
	
	public Centroid() {}
	
/*	public Centroid(int id, double x, double y, double z) {
		super(x,y,z);
		this.id = id;
	}*/
	
	public Centroid(int id, double x, double y, double z, Integer count) {
		super(x,y,z);
		this.id = id;
		this.count = count;
	}
	
	public Centroid(int id, Point p) {
		super(p.x, p.y , p.z);
		this.id = id;
	}

	public Centroid(int id, Point p, Integer count) {
		super(p.x, p.y , p.z);
		this.id = id;
		this.count = count;
	}
	@Override
	public String toString() {
		return id + "," + count + "," + super.toString();
	}

}
