package task3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class validSampleFilter  implements FilterFunction<Tuple4<Integer, Point, Double, Double>> {

	@Override
	public boolean filter(Tuple4<Integer, Point, Double, Double> value) throws Exception {
		// TODO Auto-generated method stub
		return (value.f2 <= value.f3);
	}

}
