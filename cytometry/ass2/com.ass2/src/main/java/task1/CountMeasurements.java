package task1;


import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Hello world!
 *
 */
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;

public class CountMeasurements {


	public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String cytoDir = params.getRequired("inputDir");
        if(cytoDir.charAt(cytoDir.length() - 1) != '/') {
            cytoDir = cytoDir + '/';
        }
        
        //String cytoMeasureDir = cytoDir + "large";

        // Read in the measurements CSV file
        // We're only interested in:
        // (sample, FSC-A, SSC-A)
        DataSet<Tuple3<String, Integer, Integer>> measurements =
            env.readCsvFile(cytoDir +"large/")
            	.ignoreFirstLine()
                .includeFields("11100000000000000")
                .ignoreInvalidLines()
                .types(String.class, Integer.class, Integer.class)
        		.filter(new FilterMeasure());
        

        		
        		
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
		DataSet<Tuple2<String, Integer>> mapData = measurements.map(tuple -> new Tuple2<String, Integer>(tuple.f0, 1))
        													   .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)));

        DataSet<Tuple2<String, Integer>> countSamples = mapData.groupBy(0).sum(1);
        											 
        countSamples.print();
        											         
      
                 
                               
         // Read in the experiments CSV file
        //We're only interested in:
        // (sample, researchers)
        DataSet<Tuple2<String, String>> researchers =
        		env.readCsvFile(cytoDir + "experiments.csv")
        		.ignoreFirstLine()
        		.includeFields("10000001")
        		.ignoreInvalidLines()
        		.types(String.class, String.class)
        		.filter(new FilterResearcher())
        		.flatMap(new ExperimentsFlatMap());
        


        
        researchers.print();
        
        // (sample, researchers)
        DataSet<Tuple2<String, String>> joinMesRsrch =
           researchers.join(countSamples)
                	  .where(0)
                	  .equalTo(0)
                	  .projectFirst(1)
                	  .projectSecond(1);
        
        DataSet<Tuple2<String, String>> aggRsrch = joinMesRsrch.groupBy(0)
        													   .sum(1)
        													   .sortPartition(1, Order.DESCENDING)
        													   .sortPartition(0, Order.ASCENDING)
        													   .setParallelism(1);
        
        //aggRsrch.print();
        //aggRsrch.writeAsCsv("/home/lohitt/put.csv", "\n","\t");
        
     
		if(params.has("output")) {
			aggRsrch.writeAsCsv(params.get("output")+"/task1.csv", "\n", "\t");
			env.execute();
		}else {
	        // Always limit direct printing
	        // as it requires pooling all resources into the driver.
	        System.err.println("No output location specified; printing first 100.");
	        aggRsrch.first(100).print();
	    }




	}
}
