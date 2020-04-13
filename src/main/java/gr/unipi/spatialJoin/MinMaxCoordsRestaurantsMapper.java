package gr.unipi.spatialJoin;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCoordsRestaurantsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private MinMaxCoordsParser parser = new MinMaxCoordsParser();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		parser.parse(value,
				Integer.parseInt(context.getConfiguration().get("restaurantLatitudeIndice")),
				Integer.parseInt(context.getConfiguration().get("restaurantLongitudeIndice")),
				context.getConfiguration().get("restaurantSeparator"));
		context.write(new Text("minLat"), new DoubleWritable(parser.getLatitude()));
		context.write(new Text("maxLat"), new DoubleWritable(parser.getLatitude()));
		context.write(new Text("minLon"), new DoubleWritable(parser.getLongitude()));
		context.write(new Text("maxLon"), new DoubleWritable(parser.getLongitude()));
	}

}
