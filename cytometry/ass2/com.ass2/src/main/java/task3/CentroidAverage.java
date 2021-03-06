package task3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CentroidAverage implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

	@Override
	public Centroid map(Tuple3<Integer, Point, Long> value) {
		return new Centroid(value.f0, value.f2, value.f1.div(value.f2));
	}
}
