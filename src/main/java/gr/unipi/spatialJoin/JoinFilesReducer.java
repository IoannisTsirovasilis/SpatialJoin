package gr.unipi.spatialJoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;

public class JoinFilesReducer extends Reducer<TextPair, Tuple4, Text, Text> {
	
	@Override
	protected void reduce(TextPair key, Iterable<Tuple4> values, Context context)
			throws IOException, InterruptedException {
		Text file1Tag = new Text("1");
		double radius = context.getConfiguration().getDouble("radius", 0);
		ArrayList<Tuple4> file1Records = new ArrayList<Tuple4>();
		for (Tuple4 value : values) {				
			if (value.getFileTag().equals(file1Tag)) {				
				file1Records.add(new Tuple4(new Text(value.getName()), new Text(value.getFileTag()), 
						new DoubleWritable(value.getLongitude().get()), new DoubleWritable(value.getLatitude().get())));
			}
			else {
				for (Tuple4 tuple: file1Records) {
					if (MathUtil.haversineDistance(tuple.getLatitude().get(), value.getLatitude().get(), 
							tuple.getLongitude().get(), value.getLongitude().get()) <= radius) {
						context.write(tuple.getName(), value.getName());
					}					
				}
			}
		}
	}
}
