package task3;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.common.aggregators.*;



public class KMClustering3 {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {
		
final ParameterTool params = ParameterTool.fromArgs(args);

		
		String inputDir = params.getRequired("inputDir");
	    
		if(inputDir.charAt(inputDir.length() - 1) != '/') {
			inputDir = inputDir + '/';
	    }
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
        DataSet<Tuple6<String, Double, Double, Double, Double, Double>> filterDataPoints = env.readCsvFile(inputDir+"large/")
                																		.ignoreFirstLine()
                																		.includeFields("11100011000100000")
                																		.ignoreInvalidLines()
                																		.types(String.class, Double.class, Double.class, Double.class, Double.class, Double.class)
                																		.filter(new FilterDataPoints());
       
        //Get DataPoint from measurements*.csv and filter invalid records and columns
        DataSet<Tuple3<Double, Double, Double>> dataPoints = filterDataPoints.project(3, 4, 5);
        
        //Converting the DataPoints to POJO
        DataSet<Point> points = dataPoints.map(tuple -> {return new Point(tuple.f0, tuple.f1, tuple.f2);}).returns(Point.class);       

        //Get Initial Centroids from InitialCentroids.class
        DataSet<Centroid> initialCentroids = InitialCentroids.getInitialCentroids(env);
        
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
		
		DataSet<Tuple3<Integer, Point, Double>> distPoints = points
										.map(new SelectNearestCentreWithDistance()).withBroadcastSet(finalCentroids, "Centroids");
		
		
		GroupReduceOperator<Tuple, Tuple> clustMaxDist = distPoints
				.project(0,2)
				.groupBy(0)
				.sortGroup(1, Order.DESCENDING)
				.first(1);
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
		DataSet<Tuple2<Integer, Double>> thresholdDist = clustMaxDist
															.map(tuple -> new Tuple2<Integer, Double>(tuple.getField(0), tuple.getField(1)))
															.returns(new TupleTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(Double.class)));
		
		thresholdDist = thresholdDist
			.map(tuple -> new Tuple2<Integer, Double>(tuple.f0, tuple.f1*0.90))
			.returns(new TupleTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(Double.class)));
		
		DataSet<Tuple4<Integer, Point, Double, Double>> joinedSet = distPoints
			.joinWithTiny(thresholdDist)
			.where(0)
			.equalTo(0)
			.projectFirst(0,1,2)
			.projectSecond(1);
		
		joinedSet = joinedSet.filter(new validSampleFilter());
		
		DataSet<Point> filteredPoints = joinedSet.map(tuple -> {return tuple.f1;}).returns(Point.class);
		
        //Get Initial Centroids from InitialCentroids.class
        DataSet<Centroid> initialCentroids2 = InitialCentroids.getInitialCentroids(env);
        
		// set number of iterations for KMeans algorithm
		IterativeDataSet<Centroid> loopCentroids2 = initialCentroids2.iterate(10);

		
		DataSet<Centroid> newCentroids2 = filteredPoints 
										 //compute closest Centroid for each input
										 .map(new SelectNearestCentre()).withBroadcastSet(loopCentroids2, "Centroids")
										 .map(new CountAppender())
										 .groupBy(0)
										 .reduce(new CountAccumulator())
										 .map(new CentroidAverage());
		
		DataSet<Centroid> finalNewCentroids = loopCentroids2.closeWith(newCentroids2);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		SortPartitionOperator output = finalNewCentroids
										.map(centroid -> new Tuple5<Integer, Long, Double, Double, Double>(centroid.id, centroid.cnt, centroid.x, centroid.y, centroid.z))
										.returns(new TupleTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(Long.class), TypeInformation.of(Double.class), TypeInformation.of(Double.class), TypeInformation.of(Double.class)))
										.partitionByRange(0)
										.setParallelism(1)
										.sortPartition(0, Order.ASCENDING);
										
		if(params.has("output")) {
			output.writeAsCsv(params.get("output")+"/task3.csv", "\n", "\t");
			env.execute();
		}else {
	        // Always limit direct printing
	        // as it requires pooling all resources into the driver.
	        System.err.println("No output location specified; printing first 100.");
	        output.first(100).print();
	    }

	}

}