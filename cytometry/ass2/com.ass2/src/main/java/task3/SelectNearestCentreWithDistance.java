package task3;


import java.util.Collection;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public  class SelectNearestCentreWithDistance extends RichMapFunction<Point, Tuple3<Integer, Point, Double>> {

	private Collection<Centroid> centroids;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		// TODO Auto-generated method stub
		this.centroids = getRuntimeContext().getBroadcastVariable("Centroids");
	}
	
	@Override
	public Tuple3<Integer, Point, Double> map(Point p) throws Exception {
		// TODO Auto-generated method stub
		
		double minDistance = Double.MAX_VALUE;
		int closestCentroidId = -1;
		
		// check all cluster centers
		for (Centroid centroid : centroids) {
			// compute distance	
			double distance = p.euclideanDistance(centroid);
			
			// update nearest cluster if necessary 
			if (distance < minDistance) {
				minDistance = distance;
				closestCentroidId = centroid.id;
			}
		}
		
		return new Tuple3<Integer, Point, Double>(closestCentroidId, p, minDistance);

	}

}
