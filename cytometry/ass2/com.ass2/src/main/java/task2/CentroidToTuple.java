package task2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class CentroidToTuple implements MapFunction<Centroid, Tuple5<Integer, Integer, Double, Double, Double >> {

	@Override
	public Tuple5<Integer, Integer, Double, Double, Double> map(Centroid value) throws Exception {
		// TODO Auto-generated method stub
		
		
		return new Tuple5<Integer, Integer, Double, Double, Double>(value.id, value.count, value.x, value.y, value.z);
	}

}
