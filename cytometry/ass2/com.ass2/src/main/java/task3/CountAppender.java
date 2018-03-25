package task3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

	@Override
	public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
		return new Tuple3<Integer, Point, Long>(t.f0, t.f1, 1L);
	} 
}
