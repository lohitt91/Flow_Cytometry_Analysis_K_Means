package task1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class ExperimentsFlatMap implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

	@Override
	public void flatMap(Tuple2<String, String> tuple, Collector<Tuple2<String, String>> splitTuple) throws Exception {
		// TODO Auto-generated method stub
		
		String research = tuple.f1;
		String[] researchArray = research.split(";");
		
		for (String researchValue : researchArray){
			
			splitTuple.collect(new Tuple2<String, String>(tuple.f0, researchValue.trim()));
		}
		
		
		
	}
	
	

}
