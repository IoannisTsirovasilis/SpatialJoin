package gr.unipi.spatialJoin.minMaxCoords;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import gr.unipi.spatialJoin.minMaxCoords.MinMaxCoordsHotelsMapper;
import gr.unipi.spatialJoin.minMaxCoords.MinMaxCoordsReducer;

public class AppMinMaxCoords {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		conf.set("hotelLatitudeIndice", "4");
		conf.set("hotelLongitudeIndice", "5");
		conf.set("hotelSeparator", "|");
		conf.set("restaurantLatitudeIndice", "3");
		conf.set("restaurantLongitudeIndice", "4");
		conf.set("restaurantSeparator", "|");
		
		Job job = Job.getInstance(conf);
		job.setJobName("Find min/max coords of hotels and restaurants");
		job.setJarByClass(AppMinMaxCoords.class);

		Path hotelsPath = new Path(args[0]);
		Path restaurantsPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(job, hotelsPath, TextInputFormat.class, MinMaxCoordsHotelsMapper.class);
		MultipleInputs.addInputPath(job, restaurantsPath, TextInputFormat.class, MinMaxCoordsRestaurantsMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(MinMaxCoordsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
