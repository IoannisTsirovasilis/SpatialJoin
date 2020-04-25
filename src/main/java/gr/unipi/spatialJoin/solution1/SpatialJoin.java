package gr.unipi.spatialJoin.solution1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import gr.unipi.spatialJoin.JobBuilder;
import gr.unipi.spatialJoin.JoinFilesReducer;
import gr.unipi.spatialJoin.TextPair;

public class SpatialJoin extends Configured implements Tool {

	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			JobBuilder.printUsage(this, "<input1> <input2> <radius> <output> <job name>");
			return -1;
		}
		Job job = Job.getInstance();
		job.setJobName(args[4]);
		job.setJarByClass(getClass());

		Path file1InputPath = new Path(args[0]);
		Path file2InputPath = new Path(args[1]);
		Path outputPath = new Path(args[3]);
		
		job.getConfiguration().setDouble("radius", Double.parseDouble(args[2]));
		
		// data set spatial borders
		job.getConfiguration().setDouble("minLat", -42.118331);
		job.getConfiguration().setDouble("maxLat", 49.00106);
		job.getConfiguration().setDouble("minLon", -124.586307);
		job.getConfiguration().setDouble("maxLon", 47.669728);
		job.getConfiguration().setInt("hSectors", 100);
		job.getConfiguration().setInt("vSectors", 100);
		
		// file 1 column indices of interest
		job.getConfiguration().setInt("file1NameIndice", 1);
		job.getConfiguration().setInt("file1LatitudeIndice", 4);
		job.getConfiguration().setInt("file1LongitudeIndice", 5);
		job.getConfiguration().set("file1Separator", "|");
		
		// file 2 column indices of interest
		job.getConfiguration().setInt("file2NameIndice", 1);
		job.getConfiguration().setInt("file2LatitudeIndice", 3);
		job.getConfiguration().setInt("file2LongitudeIndice", 4);
		job.getConfiguration().set("file2Separator", "|");
		
		MultipleInputs.addInputPath(job, file1InputPath, TextInputFormat.class, JoinFile1Mapper.class);
		MultipleInputs.addInputPath(job, file2InputPath, TextInputFormat.class, JoinFile2Mapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// Partitioner
		job.setPartitionerClass(KeyPartitioner.class);
		
		// Key Comparator
		job.setSortComparatorClass(TextPair.KeyComparator.class);
		
		// Group Comparator
		job.setGroupingComparatorClass(TextPair.GroupComparator.class);

		// Map conf
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);
		
		// Reducer conf
		job.setReducerClass(JoinFilesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
