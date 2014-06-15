package HashToDisk;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.google.common.base.Splitter;

public class SplitBenchmark {
	public static void Run(int loopCount)
	{
		String row = "5999968|12896|F|354575.46|1992-12-24|3-MEDIUM|Clerk#000000736|0| cajole blithely ag|";
		Iterable<String> is;
		String ok; 
		StringTokenizer st;
		String[] columns;
		List<String> sl = new ArrayList<String>();
		
		long startSplitter = System.nanoTime(); 
		
		for (int i=0;i < loopCount ; i++)
		{
			is = Splitter.on('|').split(row);
/*			while (is.iterator().hasNext())
			{
				sl.add(is.iterator().next());
			}
*/			
		}
		
		double elapsedSplitter = (System.nanoTime() - startSplitter) * 1.0e-6;
		System.out.println("Splitter " + elapsedSplitter + " msec " );
		long startTokenizer = System.nanoTime(); 
		for (int i=0;i < loopCount ; i++)
		{
			st = new StringTokenizer(row,"|");
			while (st.hasMoreTokens())
			{
				sl.add(st.nextToken());
			}
			sl.clear();
		}	
		
		double elapsedTokeniezer = (System.nanoTime() - startTokenizer) * 1.0e-6;
		System.out.println("StringTokenizer " + elapsedTokeniezer + " msec " );
		
		long startSplit = System.nanoTime(); 

		for (int i=0;i < loopCount ; i++)
		{
			columns =row.split("\\|");
			ok  = columns[0];
		}	
		
		double elapsedSplit = (System.nanoTime() - startSplit) * 1.0e-6;
		System.out.println("Split " + elapsedSplit + " msec " );
	}
}
