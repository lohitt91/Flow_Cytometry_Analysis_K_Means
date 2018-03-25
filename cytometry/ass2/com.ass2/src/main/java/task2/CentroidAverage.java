package task2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CentroidAverage implements MapFunction<Tuple3<Integer, Point, Integer>, Centroid> {

	@Override
	public Centroid map(Tuple3<Integer, Point, Integer> value) {
		return new Centroid(value.f0, value.f1.div(value.f2), value.f2);
	}
}
