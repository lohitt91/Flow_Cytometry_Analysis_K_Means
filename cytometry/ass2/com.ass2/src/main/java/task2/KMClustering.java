package task2;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;



public class KMClustering {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String cytoDir = params.getRequired("inputDir");
        if(cytoDir.charAt(cytoDir.length() - 1) != '/') {
            cytoDir = cytoDir + '/';
        }
				
        DataSet<Tuple6<String, Double, Double, Double, Double, Double>> filterDataPoints = env.readCsvFile(cytoDir + "large/")
                																		.ignoreFirstLine()
                																		.includeFields("11100011000100000")
                																		.ignoreInvalidLines()
                																		.types(String.class, Double.class, Double.class, Double.class, Double.class, Double.class)
                																		.filter(new FilterDataPoints());
       
        
        //Get DataPoint from measurements*.csv and filter invalid records and columns
        DataSet<Tuple3<Double, Double, Double>> dataPoints = filterDataPoints.project(3, 4, 5);
        
        //Converting the DataPoints to POJO
        DataSet<Point> points = dataPoints.map(tuple -> {return new Point(tuple.f0, tuple.f1, tuple.f2);}).returns(Point.class);
        //dataPoints.print();
        //System.out.println("\n"+"\n");
        
        

        //Get Initial Centroids from InitialCentroids.class
        DataSet<Centroid> initialCentroids = InitialCentroids.getInitialCentroids(env);
        
        //initialCentroids.print();
        
		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Centroid> loopCentroids = initialCentroids.iterate(10);
		
		
		DataSet<Centroid> newCentroids = points 
										 //compute closest Centroid for each input
										 .map(new SelectNearestCentre()).withBroadcastSet(loopCentroids, "Centroids")
										 .map(new CountAppender())
										 .groupBy(0)
										 .reduce(new CountAccumulator())
										 .map(new CentroidAverage());
		
		DataSet<Centroid> finalCentroids = loopCentroids.closeWith(newCentroids);
		
		//DataSet<Tuple5<Integer, Integer, Double, Double, Double>> centroidTupleSet = finalCentroids.map(new CentroidToTuple()); 																				.returns(Tuple5<Integer, Integer, Double, Double, Double>.class) ;
		
		DataSet<Tuple5<Integer, Integer, Double, Double, Double>> centroidTupleSet = finalCentroids.map(new CentroidToTuple());
		
		//DataSet<Tuple1<Centroid>> cen = finalCentroids.map(tuple -> new Tuple1<Centroid>(Centroid));
		

		centroidTupleSet = centroidTupleSet.sortPartition(0, Order.ASCENDING).setParallelism(1);
		//centroidTupleSet.writeAsCsv("/home/lohitt/centroids.csv", "\n",",").setParallelism(1);
		//centroidTupleSet.writeAsCsv("/home/lohitt/centroids_tab.csv", "\n","\t").setParallelism(1);
		
		//finalCentroids.writeAsText("/home/lohitt/centroids/");
		
/*		DataSet<Tuple3<Integer, Point, Double>> distancePoints = points
										  //compute Distance for each input with clusterID
										  .map(new CalculateDistance()).withBroadcastSet(finalCentroids, "FinalCentroids");
		
		
		distancePoints.writeAsCsv("/home/lohitt/distance.csv", "\n",",").setParallelism(1);*/
		
		//finalCentroids.print();
		
		//centroidTupleSet.writeAsCsv("/home/lohitt/centroids", "\n","\t");
		//centroidTupleSet.writeAsCsv("/home/lohitt/centroids", "\n","\t", WriteMode.);
		//env.execute();
		
		if(params.has("output")) {
			centroidTupleSet.writeAsCsv(params.get("output")+"/task2.csv", "\n", "\t").setParallelism(1);
			env.execute();
		}else {
	        // Always limit direct printing
	        // as it requires pooling all resources into the driver.
	        System.err.println("No output location specified; printing first 100.");
	        centroidTupleSet.first(100).print();
	    }
       
	}

}
