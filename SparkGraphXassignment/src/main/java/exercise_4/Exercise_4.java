package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.rdd.RDD;
import org.graphframes.GraphFrame;
import java.util.stream.IntStream;
import java.util.ArrayList;
import java.util.List;
import utils.Utils;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

		JavaRDD<String> vertex = ctx.textFile("src/main/resources/wiki-vertices.txt");
		JavaRDD<String> edge = ctx.textFile("src/main/resources/wiki-edges.txt");

		StructType verticesSchema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, false, new MetadataBuilder().build()),
				new StructField("title", DataTypes.StringType, false, new MetadataBuilder().build())
		});

		StructType edgesSchema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, false, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, false, new MetadataBuilder().build())
		});

		RDD<Row> vertexRDD = vertex.map(r -> RowFactory.create(r.split("\t")[0], r.split("\t")[1])).rdd();
		RDD<Row> edgeRDD = edge.map(r -> RowFactory.create(r.split("\t")[0], r.split("\t")[1])).rdd();

		Dataset<Row> vertices = sqlCtx.createDataFrame(vertexRDD, verticesSchema);
		Dataset<Row> edges = sqlCtx.createDataFrame(edgeRDD, edgesSchema);

		GraphFrame gf = GraphFrame.apply(vertices, edges);

		// Initializing a list to store the time taken for each configuration
		List<Row> timeList = new ArrayList<Row>();

		// Defining the output schema for the data
		StructType outputSchema = new StructType(new StructField[]{
				new StructField("damping_factor", DataTypes.DoubleType, false, new MetadataBuilder().build()),
				new StructField("reset_probability (1 - damping_factor)", DataTypes.DoubleType, false, new MetadataBuilder().build()),
				new StructField("max_iterations", DataTypes.IntegerType, false, new MetadataBuilder().build()),
				new StructField("time_taken_seconds", DataTypes.LongType, false, new MetadataBuilder().build())
		});

		// Iterating over damping factor values from 0.05 to 0.95 in steps of 0.05
		for (int i = 1; i < 20; i++) {
			double dampingFactor = i * 0.05;
			double resetProbability = 1 - dampingFactor;

			// Iterating over max iteration values from 5 to 20 in steps of 5
			for (int j = 1; j < 5; j++) {
				int maxIterations = j * 5;

				// Running PageRank on the graph using current configuration
				long startTime = System.currentTimeMillis();
				GraphFrame graphFrame = gf.pageRank().resetProbability(resetProbability).maxIter(maxIterations).run();
				long endTime = System.currentTimeMillis();

				// Calculating the time taken for the current configuration in seconds
				double timeTakenSeconds = (endTime - startTime) / 1000.0;

				// Storing the configuration and the time taken in the list
				timeList.add(RowFactory.create(dampingFactor, resetProbability, maxIterations, timeTakenSeconds));

				// Sorting vertices by pagerank and displaying top 10 vertices
				Dataset<Row> topVertices = graphFrame.vertices().sort(org.apache.spark.sql.functions.desc("pagerank"));
				String log = "Configuration: damping factor = " + dampingFactor + ", max iterations = " + maxIterations + ", time taken = " + timeTakenSeconds + " seconds\n\n";
				System.out.println(log);
				topVertices.show(10);
			}
		}
		Dataset<Row> output = sqlCtx.createDataFrame(ctx.parallelize(timeList), outputSchema);
		// Writing the output as a CSV file
		output.write().mode(SaveMode.Overwrite).option("header", true).csv("pageRank_log.csv");
	}
}
