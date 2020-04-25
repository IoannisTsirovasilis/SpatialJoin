package gr.unipi.spatialJoin.minMaxCoords;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCoordsHotelsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private MinMaxCoordsParser parser = new MinMaxCoordsParser();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		parser.parse(value,
				context.getConfiguration().getInt("hotelLatitudeIndice", -1),
				context.getConfiguration().getInt("hotelLongitudeIndice", -1),
				context.getConfiguration().get("hotelSeparator"));
		context.write(new Text("minLat"), new DoubleWritable(parser.getLatitude()));
		context.write(new Text("maxLat"), new DoubleWritable(parser.getLatitude()));
		context.write(new Text("minLon"), new DoubleWritable(parser.getLongitude()));
		context.write(new Text("maxLon"), new DoubleWritable(parser.getLongitude()));
	}

}
