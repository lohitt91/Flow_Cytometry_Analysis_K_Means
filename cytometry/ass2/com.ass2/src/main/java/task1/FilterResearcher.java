package task1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FilterResearcher implements FilterFunction<Tuple2<String, String>>{

	@Override
	public boolean filter(Tuple2<String, String> value) throws Exception {
		// TODO lAuto-generated method stub
		
		return !value.f1.isEmpty();
	}
	
	

}
