package gr.unipi.spatial_join;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCoordsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private MinMaxCoordsParser parser = new MinMaxCoordsParser();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		parser.parse(value,
				Integer.parseInt(context.getConfiguration().get("hotelLatitudeIndice")),
				Integer.parseInt(context.getConfiguration().get("hotelLongitudeIndice")),
				context.getConfiguration().get("hotelSeparator"));
		context.write(new Text("minLat"), new DoubleWritable(parser.getLatitude()));
		context.write(new Text("maxLat"), new DoubleWritable(parser.getLatitude()));
		context.write(new Text("minLon"), new DoubleWritable(parser.getLongitude()));
		context.write(new Text("maxLon"), new DoubleWritable(parser.getLongitude()));
	}

}
