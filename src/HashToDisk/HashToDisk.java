package HashToDisk;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/*
 *  The query will be 
 *  select count(*), sum(L_EXTENDEDPRICE) from ORDERS, LINEITEM  
 *  where L_ORDERKEY = O_ORDERKEY
 *  and O_ORDERDATE between 1995-01-02 and 1996-01-02 
 * 
 * 
 */

public class HashToDisk {

	// Hashing function
	HashFunction hasher_32 = Hashing.murmur3_32();
	
	// Hash table parameters
	int numberOfHashBuckets;	// Use 16 hash buckets
	int estimateNumberOfRows; 	// CE is 10e6 for build size
	int maxInmemoryRowCount; 	// 50MB with row size of 64 bytes 
	int maxInMemorySize; 		// 50MB
	int[] bucketsCount ;
	
	// Bloom filter
	BloomFilter<Integer> bf;
	
	// Consider not building the bloom filter again for spilled data
	// as the filter has already been applied, so most likely it won't 
	// provide much benefit
	boolean buildBloomfilter;
	
	// Hash tables
	HashMap<Integer, orderkeyRow> buildSideHashTable;
	SpillingHashTable<Integer, orderkeyRow> onDiskHt; 
	
	// Date formatter
	SimpleDateFormat myFormat; 
	
	// I/O Files
	String probeSideFilePath;
	String buildSideFilePath;
	String spillFilesFolder;
	FileReader buildSideFileReader;
	FileReader probeSideFileReader;
	
	// Used in-case we need to spill
	SpillFileHandler[] probSideSpillFiles;
	
	public HashToDisk(	String probeSideFilePath,	// Path to build side flat file
						String buildSideFilePath,	// Path to probe side flat file
						String spillFilesFolder,	// Path to where we spill
						int numberOfHashBuckets,	// Use 16 hash buckets
						int estimateNumberOfRows, 	// CE is 10e6 for build size
						int maxInmemoryRowCount, 	// 50MB with row size of 64 bytes 
						int maxInMemorySize 		// 50MB
					) throws IOException
	{
		myFormat = new SimpleDateFormat("yyyy-MM-dd");
		bucketsCount = new int[numberOfHashBuckets];
		
		for(int i = 0; i < numberOfHashBuckets;i++)
		{
			bucketsCount[i] = 0;
		}
	
		// Save the file paths
		this.probeSideFilePath = probeSideFilePath;
		this.buildSideFilePath = buildSideFilePath;
		this.spillFilesFolder = spillFilesFolder;
		
		// Save the hash table parameters
		this.numberOfHashBuckets = numberOfHashBuckets;
		this.estimateNumberOfRows = estimateNumberOfRows;
		this.maxInmemoryRowCount = maxInmemoryRowCount;
		this.maxInMemorySize = maxInMemorySize;
		
		buildSideFileReader = new FileReader(this.buildSideFilePath);
		probeSideFileReader = new FileReader(this.probeSideFilePath);
		
		// Bloom filters have relatively small size compared, if the estimate is way off
		// they become ineffective with very high false positive rate
		// So compensate for the possibility of having low cardinality
		bf = BloomFilter.create(Funnels.integerFunnel(), estimateNumberOfRows * 5);
		
		// Default hash map
		buildSideHashTable = new HashMap<Integer, orderkeyRow>(); 
		
		// Hash map which would spill partitions to disk
		onDiskHt = new SpillingHashTable<Integer, orderkeyRow>(
							this.spillFilesFolder,					// path for spill files
							orderkeyRow.GetEstimeSize(),		// rowSize in bytes
							numberOfHashBuckets,				// numberOfHashBuckets
							maxInmemoryRowCount,				// maxInmemoryRowCount
							maxInMemorySize,					// maxInMemorySize in bytes
							estimateNumberOfRows,				// estimeRowCount
							-1									// initialCapacity
							);
	}
	
	/**
	 * Reads the build side flat file and populate the hash table
	 * @throws ParseException
	 */
	public void ProcessBuildSide() throws ParseException
	{
		int hashBucketID = 0;
		int buildQualifyingRows = 0;
		int buildRowCount = 0;
		List<String> o_rows;
		String[] columns;
		int o_orderkey;
		Date o_orderdate;
		String minDateFilter = "1995-01-02";
		String maxDateFilter = "1996-02-02";
		Date minOrderDate = myFormat.parse(minDateFilter);
		Date maxOrderDate = myFormat.parse(maxDateFilter);
		
		// Capture start time for build side
		long startBuildPhase = System.nanoTime(); 
		
		// Read the entire file or (build side)
		while(buildSideFileReader.HasNext())
		{
			// Get a batch of rows
			o_rows = buildSideFileReader.GetNextBatch();
			
			// Now Hash o_orderkey
			for (String row : o_rows) {

				// Split the row to get the columns
				columns = row.split("\\|");

				// Parse out orderkey and orderdate
				o_orderkey = Integer.parseInt(columns[0]);
				o_orderdate = myFormat.parse(columns[4]);
				
				// hash orderkey and try to figure which hash bucket it should be in
				// & with 0x7fffffff to avoid getting negative indexes into the arry
				hashBucketID = (hasher_32.hashInt(o_orderkey).asInt() & 0x7fffffff) % numberOfHashBuckets;
				++bucketsCount[hashBucketID];
				
				// Apply the filter
				if (o_orderdate.after(minOrderDate) & o_orderdate.before(maxOrderDate))
				{
					// If the row qualifies insert into hash table and bloom filter
					// Gains from bloom filter will only benefit when spilling occurs
					bf.put(o_orderkey);
					buildSideHashTable.put(o_orderkey, new orderkeyRow(o_orderkey, o_orderdate));
					onDiskHt.InsertIntoHashMap(hashBucketID, o_orderkey, new orderkeyRow(o_orderkey, o_orderdate));
					buildQualifyingRows++;
				}
				buildRowCount++;
			}
		}
		
		// Capture end time of build phase
		double elapsedTimeInMsecBuildSide = (System.nanoTime() - startBuildPhase) * 1.0e-6;
		
		System.out.println("\n" + "Build side qualified rows "+ buildQualifyingRows);
		System.out.println("Build side elapsed " + buildRowCount + " rows scanned, in " + elapsedTimeInMsecBuildSide + " msec, " + buildRowCount / elapsedTimeInMsecBuildSide * 1000 + " rows/sec" + "\n" );

	}
	
	public ResultSet ProcessProbSideQuickPath()
	{
		int l_orderkey;
		Double l_extendedPrice;
		Double summExtendedPrice = 0.0;
		List<String> l_rows;
		String[] columns;
		int probeRowCount = 0;
		int bloomQualifyingRows = 0;
		int probeQualifyingRowsBaseHt = 0;
		int probeQualifyingBucketHt = 0;
		ResultSet queryResult;
		
		// Capture start time for build side
		long startProbePhase = System.nanoTime(); 
		
		// Now do the probing , just check against bloom filter for now
		while(probeSideFileReader.HasNext())
		{
			l_rows = probeSideFileReader.GetNextBatch();
			for (String row : l_rows)
			{
				columns = row.split("\\|");
				l_orderkey = Integer.parseInt(columns[0]);
				if (bf.mightContain(l_orderkey))
				{
					bloomQualifyingRows++;
					if (buildSideHashTable.containsKey(l_orderkey))
					{
						probeQualifyingRowsBaseHt++;	
					}
					
					if (onDiskHt.containsKey(l_orderkey))
					{
						l_extendedPrice = Double.parseDouble(columns[5]);
						summExtendedPrice += l_extendedPrice;
						probeQualifyingBucketHt++;
					}
				}
				probeRowCount++;
			}
		}
		assert (probeQualifyingRowsBaseHt == probeQualifyingBucketHt);
		double elapsedTimeInMsecProbePhase = (System.nanoTime() - startProbePhase) * 1.0e-6;
/*
		System.out.println("Bloom filter size KB " + SizeOf.deepSizeOf(bf) / 1024);
		System.out.println("Hash table size KB " + SizeOf.deepSizeOf(buildSideHashTable) / 1024);
		System.out.println("Hash table2 size KB " + SizeOf.deepSizeOf(onDiskHt) / 1024);
*/	
		System.out.println("\n");
		System.out.println("Probe side elapsed " + probeRowCount + " rows scanned, in " + elapsedTimeInMsecProbePhase + " msec, " + probeRowCount / elapsedTimeInMsecProbePhase * 1000 + " rows/sec");

		System.out.println(String.format("Qualifying rows from Bloom filter " + bloomQualifyingRows));
		System.out.println(String.format("Qualifying rows from lineitem " + probeQualifyingRowsBaseHt));
		System.out.println(String.format("Qualifying rows from lineitem1 " + probeQualifyingBucketHt));
		
		queryResult = new ResultSet(probeQualifyingBucketHt, summExtendedPrice);
		return 	queryResult;
	}
	
	public SpillFileHandler[]  HashPartitionFileToDisk (FileReader fileReader) throws IOException
	{
		int hashBucketID = 0;
		int bloomQualifyingRows = 0;
		int probeSpillRowCount = 0;
		int l_orderkey;
		Double l_extendedPrice;
		List<String> l_rows;
		String[] columns;
		SpillFileHandler[] spillFileHandler = new SpillFileHandler[numberOfHashBuckets];
		
		// Initialize the files we will spill to
		probSideSpillFiles = new SpillFileHandler[numberOfHashBuckets];
		for (int i = 0; i < numberOfHashBuckets; i++)
		{
			spillFileHandler[i] =  new SpillFileHandler(spillFilesFolder + "probeSideSpillFile" + i + ".txt");
		}
		
		// Capture start time for build side
		long startSPillProbePhase = System.nanoTime(); 
		
		// Now do the probing , just check against bloom filter for now
		while(fileReader.HasNext())
		{
			l_rows = probeSideFileReader.GetNextBatch();
			for (String row : l_rows)
			{
				columns = row.split("\\|");
				l_orderkey = Integer.parseInt(columns[0]);
				
				if (bf.mightContain(l_orderkey))
				{
					l_extendedPrice = Double.parseDouble(columns[5]);
					hashBucketID = (hasher_32.hashInt(l_orderkey).asInt() & 0x7fffffff) % numberOfHashBuckets;
					if (onDiskHt.DidPartitionSpill(hashBucketID))
					{
						spillFileHandler[hashBucketID].writeToFile(l_orderkey +","+l_extendedPrice);
						bloomQualifyingRows++;
						probeSpillRowCount++;
					} 
					else
					{
						onDiskHt.containsKey(l_orderkey, hashBucketID);
					}
				}
			}
		}
		
		// Flush the data and close the file
		for (int i = 0; i < numberOfHashBuckets; i++)
		{
			spillFileHandler[i].CloseFile();
		}
		
		double elapsedTimeInMsecSpillProbePhase = (System.nanoTime() - startSPillProbePhase) * 1.0e-6;
		System.out.println("Probe side spill elapsed " + probeSpillRowCount + " rows , in " + elapsedTimeInMsecSpillProbePhase + " msec, " + probeSpillRowCount / elapsedTimeInMsecSpillProbePhase * 1000 + " rows/sec");
		
		return spillFileHandler;
	}
	
	public void ProcessProbSideSlowPath () throws IOException
	{
		// Hash partition the qualifying rows from the probe side to disk
		SpillFileHandler[] probeSideSpillFileHandler = HashPartitionFileToDisk(probeSideFileReader);
		
		// Get the spill files from the build side
		SpillFileHandler[] buildSideSpillFileHandler = onDiskHt.GetBuildSideSpillFileHandlers();
		
		for (int i =0 ; i < numberOfHashBuckets; i++)
		{
			if (onDiskHt.DidPartitionSpill(i))
			{
				FileReader buildSideFileReader = new FileReader(buildSideSpillFileHandler[i].getFileName());
				FileReader probSideFileReader = new FileReader(probeSideSpillFileHandler[i].getFileName());
				
			}
		}
		
	}
	
	public void ProcessHashJoin() throws ParseException, IOException {
		
		ResultSet rs;
		// Read data from probe side and insert into N hash tables
		ProcessBuildSide();
		
		if (!onDiskHt.HasSpilledData())
		{
			rs = ProcessProbSideQuickPath();
		}
		else
		{
			ProcessProbSideSlowPath();
		}
		
		onDiskHt.DumpRowsInHashBuckets();
		
		for(int i = 0; i < numberOfHashBuckets;i++)
		{
			System.out.println(String.format("Hash1: " + i + " has count of " + bucketsCount[i]));
		}
		
		probeSideFileReader.closefile();
		buildSideFileReader.closefile();
	}

	public static void main(String[] args) throws ParseException, IOException {

		String probeSideFilePath = "//Users//mmokhtar//Downloads//tpch_2_17_0//dbgen//TPCH//lineitem.tbl";
		String buildSideFilePath = "//Users//mmokhtar//Downloads//tpch_2_17_0//dbgen//TPCH//orders.tbl";
		String spillFilesFolder = "//Users//mmokhtar//Downloads//spillData//";
		int numberOfHashBuckets = 16;		// Use 16 hash buckets
		int estimateNumberOfRows = 1000000; // CE is 10e6 for build size
		int maxInmemoryRowCount = 70000; 	// 50MB with row size of 64 bytes 70000 will spill
		int maxInMemorySize = 52428800; 	// 50MB
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//SplitBenchmark.Run(1000000);
		//while (true) 		{
			HashToDisk ht = new HashToDisk(	probeSideFilePath,
											buildSideFilePath,
											spillFilesFolder,
											numberOfHashBuckets,
											estimateNumberOfRows,
											maxInmemoryRowCount,
											maxInMemorySize);
			ht.ProcessHashJoin();
//		}
	}
}
