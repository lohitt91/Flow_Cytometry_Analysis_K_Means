package task1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class FilterMeasure implements FilterFunction<Tuple3<String, Integer, Integer>> {

	@Override
	public boolean filter(Tuple3<String, Integer, Integer> value) throws Exception {
		// TODO Auto-generated method stub
		
		return ((value.f1 >= 1 && value.f1 <= 150000) && (value.f2 >= 1 && value.f2 <= 150000));
	}

}
