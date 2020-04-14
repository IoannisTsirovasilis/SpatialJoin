package gr.unipi.spatialJoin;

import org.apache.hadoop.util.ToolRunner;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
	    System.exit(exitCode);
	}
}