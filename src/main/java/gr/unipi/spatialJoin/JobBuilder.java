package gr.unipi.spatialJoin;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class JobBuilder {
	public static void printUsage(Tool tool, String extraArgsUsage) {
		System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
}
