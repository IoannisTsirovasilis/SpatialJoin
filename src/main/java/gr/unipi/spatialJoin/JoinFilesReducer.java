package gr.unipi.spatialJoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;

public class JoinFilesReducer extends Reducer<TextPair, TextPair, Text, Text> {
	
	@Override
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context)
			throws IOException, InterruptedException {
		Text file1Tag = new Text("1");
		ArrayList<Text> file1Names = new ArrayList<Text>();
		for (TextPair value : values) {				
			if (value.getSecond().equals(file1Tag)) {
				
				file1Names.add(new Text(value.getFirst()));
			}
			else {
				for (Text name : file1Names) {	
					context.write(name, value.getFirst());
				}
			}
		}
	}
}
