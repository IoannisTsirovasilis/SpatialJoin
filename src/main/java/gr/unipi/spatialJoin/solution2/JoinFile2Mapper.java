package gr.unipi.spatialJoin.solution2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gr.unipi.spatialJoin.GridBuilder;
import gr.unipi.spatialJoin.GridCell;
import gr.unipi.spatialJoin.RecordParser;
import gr.unipi.spatialJoin.TextPair;

public class JoinFile2Mapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
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
				context.getConfiguration().getInt("file2NameIndice", -1),
				context.getConfiguration().getInt("file2LatitudeIndice", -1),
				context.getConfiguration().getInt("file2LongitudeIndice", -1),
				context.getConfiguration().get("file2Separator"));
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
		int j = (int) Math.floor(gridBuilder.getHSectors() * (parser.getLongitude() - minLon) / (maxLon - minLon));
		int i = (int) Math.floor(gridBuilder.getVSectors() * (parser.getLatitude() - minLat) / (maxLat - minLat));
		if (i == gridBuilder.getVSectors()) {
			i--;
		}
		if (j == gridBuilder.getHSectors()) {
			j--;
		}
		context.write(new TextPair(cells[i][j].getId(), "2"), new TextPair(parser.getName(), "2"));
		
		if (i == 0 && j == 0) {
			if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i + 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			return;
		}
		
		if (i == 0 && j == cells[0].length - 1) {
			if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i + 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			return;
		}
		
		if (i == 0) {
			if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i + 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i + 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			return;
		}
		
		// ------------------------------------------
		
		if (i == cells.length - 1 && j == 0) {
			if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i - 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			return;
		}
		
		if (i == cells.length - 1 && j == cells[0].length - 1) {
			if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i - 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			return;
		}
		
		if (i == cells.length - 1) {
			if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i - 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i - 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			return;
		}
		
		if (j == 0) {
			if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i - 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i + 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			return;
		}
		
		if (j == cells[0].length - 1) {
			if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i - 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i - 1][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i + 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i + 1][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
				context.write(new TextPair(cells[i][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
			}
			
			return;
		}
		
		if (cells[i + 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
			context.write(new TextPair(cells[i + 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
		}
		
		if (cells[i - 1][j].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
			context.write(new TextPair(cells[i - 1][j].getId(), "2"), new TextPair(parser.getName(), "2"));
		}
		
		if (cells[i - 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
			context.write(new TextPair(cells[i - 1][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
		}
		
		if (cells[i + 1][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
			context.write(new TextPair(cells[i + 1][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
		}
		
		if (cells[i][j - 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
			context.write(new TextPair(cells[i][j - 1].getId(), "2"), new TextPair(parser.getName(), "2"));
		}
		
		if (cells[i][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
			context.write(new TextPair(cells[i][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
		}
		
		if (cells[i - 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
			context.write(new TextPair(cells[i - 1][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
		}
		
		if (cells[i + 1][j + 1].isInDistance(parser.getLatitude(), parser.getLongitude(), radius)) {
			context.write(new TextPair(cells[i + 1][j + 1].getId(), "2"), new TextPair(parser.getName(), "2"));
		}
	}
}

