package task2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple6;

public class FilterDataPoints  implements FilterFunction<Tuple6<String, Double, Double, Double, Double, Double>> {

	@Override
	public boolean filter(Tuple6<String, Double, Double, Double, Double, Double> value) throws Exception {
		// TODO Auto-generated method stub
		return ((value.f1 >= 1 && value.f1 <= 150000) && (value.f2 >= 1 && value.f2 <= 150000));
	}

}
