package gr.unipi.spatialJoin.solution1;

import org.apache.hadoop.util.ToolRunner;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SpatialJoin(), args);
	    System.exit(exitCode);
	}
}