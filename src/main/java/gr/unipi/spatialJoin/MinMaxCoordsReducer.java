package gr.unipi.spatialJoin;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCoordsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<DoubleWritable> iter = values.iterator();
		DoubleWritable coord = iter.next();
		
		if (key.toString().contains("min")) {
			while (iter.hasNext()) {
				DoubleWritable temp = iter.next();
				coord.set(Math.min(coord.get(), temp.get()));
			}
		}
		else if (key.toString().contains("max")){
			while (iter.hasNext()) {
				DoubleWritable temp = iter.next();
				coord.set(Math.max(coord.get(), temp.get()));
			}
		}
		
		context.write(new Text(key.toString()), coord);
	}
}
