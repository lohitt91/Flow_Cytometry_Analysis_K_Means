package task3;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CountAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

	@Override
	public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> value1, Tuple3<Integer, Point, Long> value2)
			throws Exception {
		// TODO Auto-generated method stub
		return new Tuple3<Integer, Point, Long>(value1.f0, value1.f1.add(value2.f1), value1.f2 + value2.f2);
	}

}
