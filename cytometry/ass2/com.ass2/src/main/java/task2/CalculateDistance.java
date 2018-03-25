package task2;

import java.util.Collection;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public class CalculateDistance extends RichMapFunction<Point, Tuple3<Integer, Point, Double>> {
	
	private Collection<Centroid> finalCentroids;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		// TODO Auto-generated method stub
		this.finalCentroids = getRuntimeContext().getBroadcastVariable("FinalCentroids");
	}
	
	@Override
	public Tuple3<Integer, Point, Double> map(Point p) throws Exception {
		// TODO Auto-generated method stub
		
		double minDistance = Double.MAX_VALUE;
		int closestCentroidId = 0;
		
		// check all cluster centers
		for (Centroid centroid : finalCentroids) {
			// compute distance	
			double distance = p.euclideanDistance(centroid);
			//centroidId = centroid.id;
			
			if (distance < minDistance) {
				minDistance = distance;
				closestCentroidId = centroid.id;
			}
			}
		
		return new Tuple3<Integer, Point, Double>(closestCentroidId, p, minDistance);

	}

}
