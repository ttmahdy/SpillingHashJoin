package spillingHashJoin;

import java.io.IOException;
import java.util.List;

public class WriteNarrowFiles {
	//static String probeSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//lineitem.tbl";
	//static String buildSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//orders.tbl";

	static String narrowLineitem = "//Users//mmokhtar//Downloads//FlatFiles//30G//narrowLineitem30G.tbl";
	static String narrowOrders = "//Users//mmokhtar//Downloads//FlatFiles//30G//narrowOrders30G.tbl";
	
	static String probeSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//30G//lineitem.tbl";
	static String buildSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//30G////orders.tbl";

	
	static FileReader probeSideFileReader = new FileReader(probeSideFilePath);
	
	static void OptimizeFile() throws IOException
	{
		WriteOutOrders(probeSideFilePath,narrowLineitem,0,5);
		WriteOutOrders(buildSideFilePath,narrowOrders,0,4);
	}
	
	static void WriteOutOrders(String inputFile, String outPutFile, int c1Id, int c2Id) throws IOException
	{
		FileReader buildSideFileReader = new FileReader(inputFile);
		TempFileWriter tfo = new TempFileWriter(outPutFile, true);
		
		while(buildSideFileReader.HasNext())
		{
			// Get a batch of rows
			List<String> o_rows = buildSideFileReader.GetNextBatch();
			
			// Now Hash o_orderkey
			for (String row : o_rows) {

				// Split the row to get the columns
				String[] columns = row.split("\\|");

				// Parse out orderkey and orderdate
				String o_orderkey = columns[c1Id];
				String o_orderdate = columns[c2Id];
				tfo.writeToFile(o_orderkey+","+o_orderdate);
			}
		}
		tfo.CloseFile();
	}
}	
