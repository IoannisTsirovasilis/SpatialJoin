package gr.unipi.spatialJoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;


public class SpatialJoin extends Configured implements Tool {

	@Override
	
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage("<input1> <input2> <output> <radius> <coords file>");
			return -1;
		}
		Job job = Job.getInstance();
		job.setJobName("Join hotels with restaurants");
		job.setJarByClass(getClass());

		Path file1InputPath = new Path(args[0]);
		Path file2InputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		double radius = Double.parseDouble(args[3]);
		Path coordsPath = new Path(args[4]);
		
		MultipleInputs.addInputPath(job, file1InputPath, TextInputFormat.class, JoinFile1Mapper.class);
		MultipleInputs.addInputPath(job, file2InputPath, TextInputFormat.class, JoinFile2Mapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		

		job.setMapOutputKeyClass(TextPair.class);

		job.setReducerClass(JoinReducer.class);

		job.setOutputKeyClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public void printUsage(String extraArgsUsage) {
		System.err.printf("Usage: %s [genericOptions] %s\n\n", getClass().getSimpleName(), extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
}
