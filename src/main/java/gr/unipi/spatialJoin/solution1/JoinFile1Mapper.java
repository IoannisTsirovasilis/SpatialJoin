package gr.unipi.spatialJoin.solution1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gr.unipi.spatialJoin.GridBuilder;
import gr.unipi.spatialJoin.GridCell;
import gr.unipi.spatialJoin.RecordParser;
import gr.unipi.spatialJoin.TextPair;

public class JoinFile1Mapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
	private RecordParser parser = new RecordParser();
	private GridBuilder gridBuilder;
	private double radius = -1;
	private double minLat;
	private double maxLat;
	private double minLon;
	private double maxLon;
	private int hSectors;
	private int vSectors;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		parser.parse(value,
				context.getConfiguration().getInt("file1NameIndice", -1),
				context.getConfiguration().getInt("file1LatitudeIndice", -1),
				context.getConfiguration().getInt("file1LongitudeIndice", -1),
				context.getConfiguration().get("file1Separator"));
		if (radius == -1) {
			radius = context.getConfiguration().getDouble("radius", 0);
			minLat = context.getConfiguration().getDouble("minLat", 0);
			maxLat = context.getConfiguration().getDouble("maxLat", 0);
			minLon = context.getConfiguration().getDouble("minLon", 0);
			maxLon = context.getConfiguration().getDouble("maxLon", 0);
			hSectors = context.getConfiguration().getInt("hSectors", 0);
			vSectors = context.getConfiguration().getInt("vSectors", 0);
			gridBuilder = new GridBuilder(radius, minLat, maxLat, minLon, maxLon, hSectors, vSectors);
			gridBuilder.buildGrid();
		}
		GridCell[][] cells = gridBuilder.getGridCells();
		for (int i = 0; i < cells.length; i++) {
			for (int j = 0; j < cells[0].length; j++) {
				if (cells[i][j].contains(parser.getLatitude(), parser.getLongitude())) {
					context.write(new TextPair(cells[i][j].getId(), "1"), new TextPair(parser.getName(), "1"));
					
					if (i == 0 && j == 0) {
						if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i + 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						break;
					}
					
					if (i == 0 && j == cells[0].length - 1) {
						if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i + 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						break;
					}
					
					if (i == 0) {
						if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i + 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i + 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						break;
					}
					
					// ------------------------------------------
					
					if (i == cells.length - 1 && j == 0) {
						if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i - 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						break;
					}
					
					if (i == cells.length - 1 && j == cells[0].length - 1) {
						if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i - 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						break;
					}
					
					if (i == cells.length - 1) {
						if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i - 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i - 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						break;
					}
					
					if (j == 0) {
						if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i - 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i + 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						break;
					}
					
					if (j == cells[0].length - 1) {
						if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i - 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i - 1][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i + 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i + 1][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
							context.write(new TextPair(cells[i][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
						}
						
						break;
					}
					
					if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
						context.write(new TextPair(cells[i + 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
					}
					
					if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
						context.write(new TextPair(cells[i - 1][j].getId(), "1"), new TextPair(parser.getName(), "1"));
					}
					
					if (cells[i - 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
						context.write(new TextPair(cells[i - 1][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
					}
					
					if (cells[i + 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
						context.write(new TextPair(cells[i + 1][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
					}
					
					if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
						context.write(new TextPair(cells[i][j - 1].getId(), "1"), new TextPair(parser.getName(), "1"));
					}
					
					if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
						context.write(new TextPair(cells[i][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
					}
					
					if (cells[i - 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
						context.write(new TextPair(cells[i - 1][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
					}
					
					if (cells[i + 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
						context.write(new TextPair(cells[i + 1][j + 1].getId(), "1"), new TextPair(parser.getName(), "1"));
					}
					
					break;
				}
			}
		}
	}
}
