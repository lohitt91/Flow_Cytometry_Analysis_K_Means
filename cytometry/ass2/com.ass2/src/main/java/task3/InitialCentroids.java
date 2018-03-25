package task3;

import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.tuple.Tuple4;

public class InitialCentroids {

	public static DataSet<Centroid> getInitialCentroids(ExecutionEnvironment env){
		
		//final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Double[] sca1 = new Double[5];
		Double[]  cd11b = new Double[5];
		Double[] ly6c = new Double[5];
		
		DecimalFormat df = new DecimalFormat("#.######");
		
		for (int i = 0 ; i <5; i++){
			
			sca1[i] = Double.parseDouble(df.format(ThreadLocalRandom.current().nextDouble(-2.028257, 6.404627)));
			ly6c[i] = Double.parseDouble(df.format(ThreadLocalRandom.current().nextDouble(-2.190031, 7.350341)));
			cd11b[i] = Double.parseDouble(df.format(ThreadLocalRandom.current().nextDouble(-0.513924, 7.295227)));
		}
		
/*		return env.fromElements(new Tuple4<Integer, Double, Double, Double>(1, ly6c[0], cd11b[0], sca1[0]),
								new Tuple4<Integer, Double, Double, Double>(2, ly6c[1], cd11b[1], sca1[1]),
								new Tuple4<Integer, Double, Double, Double>(3, ly6c[2], cd11b[2], sca1[2]),
								new Tuple4<Integer, Double, Double, Double>(4, ly6c[3], cd11b[3], sca1[3]),
								new Tuple4<Integer, Double, Double, Double>(5, ly6c[4], cd11b[4], sca1[4]));*/
		
		return env.fromElements(new Centroid(1, ly6c[0], cd11b[0], sca1[0]),
				new Centroid(2, ly6c[1], cd11b[1], sca1[1]),
				new Centroid(3, ly6c[2], cd11b[2], sca1[2]),
				new Centroid(4, ly6c[3], cd11b[3], sca1[3]));
				//new Centroid(5, ly6c[4], cd11b[4], sca1[4]));
		
	}
}
