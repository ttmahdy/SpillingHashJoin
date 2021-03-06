package spillingHashJoin;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class FileReader
{
	String fileName;
	LineIterator it;
	List<String> rows = new ArrayList<String>();
	int batchSize = 10000;
	int totalRows = 0;
	
	public FileReader(String fileName)
	{
		this.fileName = fileName;
		try {
			it = FileUtils.lineIterator(new File(fileName), "ASCII");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			it = null;
		}
	}
	
	public boolean HasNext()
	{
		return it.hasNext();			
	}
	
	public String getNextRow()
	{
		String line = null;
		if (it.hasNext()) 
		{
            line = it.nextLine();
        }
		
		return line;
	}
	
	public List<String> GetNextBatch()
	{
		int rowNum = 0;
		String outLine;
		rows.clear();
		do {
			outLine = it.nextLine();
			rows.add(outLine);
			rowNum++;
			totalRows++;
			if (totalRows > 6860000)
			{
				//System.out.println(totalRows);
				//System.out.println(outLine.length());
			}
		} while (it.hasNext() && rowNum < batchSize);
		
		return rows;
	}
	
	public void closefile()
	{
		LineIterator.closeQuietly(it);
	}
}
