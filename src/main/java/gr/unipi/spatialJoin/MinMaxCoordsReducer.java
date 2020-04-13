package gr.unipi.spatialJoin;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCoordsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		
		double outputValue;
		
		if (key.toString().contains("min")) {
			outputValue = Double.MAX_VALUE;
			for (DoubleWritable value : values) {
				outputValue = Math.min(outputValue, value.get());
			}
		}
		else {
			outputValue = Double.MIN_VALUE;
			for (DoubleWritable value : values) {
				outputValue = Math.max(outputValue, value.get());
			}
		}
		
		context.write(new Text(key.toString()), new DoubleWritable(outputValue));
	}
}
