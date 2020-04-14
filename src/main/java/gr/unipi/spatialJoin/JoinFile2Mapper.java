package gr.unipi.spatialJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinFile2Mapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
	private RecordParser parser = new RecordParser();
	private GridBuilder gridBuilder;
	private double radius = -1;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		parser.parse(value,
				context.getConfiguration().getInt("file2NameIndice", -1),
				context.getConfiguration().getInt("file2LatitudeIndice", -1),
				context.getConfiguration().getInt("file2LongitudeIndice", -1),
				context.getConfiguration().get("file2Separator"));
		if (radius == -1) {
			radius = context.getConfiguration().getDouble("radius", 0);
			double minLat = context.getConfiguration().getDouble("minLat", 0);
			double maxLat = context.getConfiguration().getDouble("maxLat", 0);
			double minLon = context.getConfiguration().getDouble("minLon", 0);
			double maxLon = context.getConfiguration().getDouble("maxLon", 0);
			gridBuilder = new GridBuilder(radius, minLat, maxLat, minLon, maxLon);
		}
		
		for (GridCell[] row : gridBuilder.getGridCells()) {
			for (GridCell cell : row) {
				if (cell.contains(parser.getLatitude(), parser.getLongitude())) {
					context.write(new TextPair(cell.getId(), "2"), new TextPair(parser.getName(), "2"));
				} else if (cell.isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
					context.write(new TextPair(cell.getId(), "2"), new TextPair(parser.getName(), "2"));
				}
			}
		}
	}
}
