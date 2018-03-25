package task3;

import java.io.Serializable;

public class Point  implements Serializable{
	
	public double x, y, z;
	
	public Point() {}

	public Point(double x, double y, double z) {
		this.x = x;
		this.y = y;
		this.z = z;
	}
	
	public Point add(Point other) {
		x += other.x;
		y += other.y;
		z += other.z;
		return this;
	}
	
	public Point div(long val) {
		x /= val;
		y /= val;
		z /= val;
		return this;
	}
	
	public double euclideanDistance(Point other) {
		return Math.sqrt((x-other.x)*(x-other.x) + (y-other.y)*(y-other.y) + (z-other.z)*(z-other.z));
	}
	
	public void clear() {
		x = y = z = 0.0;
	}
	
	@Override
	public String toString() {
		return x + " " + y + " " + z;
	}

}
