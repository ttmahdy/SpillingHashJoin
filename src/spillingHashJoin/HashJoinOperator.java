package spillingHashJoin;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

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

public class HashJoinOperator {

	// Hashing function
	HashFunction hasher_32 = Hashing.murmur3_32();
	
	// Hash table parameters
	int numberOfHashBuckets;	// Use 16 hash buckets
	int estimateNumberOfRows; 	// CE is 10e6 for build size
	int maxInmemoryRowCount; 	// 50MB with row size of 64 bytes 
	int maxInMemorySize; 		// 50MB
	int[] bucketsCount ;
	int spillLevel;				// Spill level
	
	// Bloom filter
	BloomFilter<Integer> bf;
	
	// Consider not building the bloom filter again for spilled data
	// as the filter has already been applied, so most likely it won't 
	// provide much benefit
	boolean buildBloomfilter;
	
	// Hash tables
	HashMap<Integer, orderkeyRow> buildSideHashTable;
	SpillingHashTable<Integer, orderkeyRow> spillingHT; 
	
	// Date formatter
	SimpleDateFormat myFormat; 
	
	// I/O Files
	String resultSetPath;
	String probeSideFilePath;
	String buildSideFilePath;
	String spillFilesFolder;
	
	// File readers
	FileReader buildSideFileReader;
	FileReader probeSideFileReader;
	
	// Naive string filters
	String minDateFilter;
	String maxDateFilter;
	
	// Used in-case we need to spill
	//TempFileWriter[] probSideSpillFiles;
	
	// Results set writer
	TempFileWriter resultSetWriter;
	
	public HashJoinOperator(	String resultSetPath,       // Path to results file
								String probeSideFilePath,	// Path to build side flat file
								String buildSideFilePath,	// Path to probe side flat file
								String spillFilesFolder,	// Path to where we spill
								int numberOfHashBuckets,	// Use 16 hash buckets
								int estimateNumberOfRows, 	// CE is 10e6 for build size
								int maxInmemoryRowCount, 	// 50MB with row size of 64 bytes 
								int maxInMemorySize, 		// 50MB
								int spillLevel,				// Spill level
								String minDateFilter,		// Min o_orderdate filter
								String maxDateFilter		// Max o_orderdate filter

					) throws IOException
	{
		myFormat = new SimpleDateFormat("yyyy-MM-dd");
		bucketsCount = new int[numberOfHashBuckets];
		
		for(int i = 0; i < numberOfHashBuckets;i++)
		{
			bucketsCount[i] = 0;
		}
	
		// Save the file paths
		this.resultSetPath = resultSetPath;
		this.probeSideFilePath = probeSideFilePath;
		this.buildSideFilePath = buildSideFilePath;
		this.spillFilesFolder = spillFilesFolder;
		
		// Save the hash table parameters
		this.numberOfHashBuckets = numberOfHashBuckets;
		this.estimateNumberOfRows = estimateNumberOfRows;
		this.maxInmemoryRowCount = maxInmemoryRowCount;
		this.maxInMemorySize = maxInMemorySize;
		this.spillLevel = spillLevel;
		
		// Save the date filters
		this.minDateFilter = minDateFilter;
		this.maxDateFilter = maxDateFilter;
		
		buildSideFileReader = new FileReader(this.buildSideFilePath);
		probeSideFileReader = new FileReader(this.probeSideFilePath);
		
		resultSetWriter = new TempFileWriter(resultSetPath, false);
		
		// Bloom filters have relatively small size compared, if the estimate is way off
		// they become ineffective with very high false positive rate
		// So compensate for the possibility of having low cardinality
		bf = BloomFilter.create(Funnels.integerFunnel(), estimateNumberOfRows  );
		
		// Default hash map
		buildSideHashTable = new HashMap<Integer, orderkeyRow>(); 
		
		// Hash map which would spill partitions to disk
		spillingHT = new SpillingHashTable<Integer, orderkeyRow>(
							this.spillFilesFolder,					// path for spill files
							orderkeyRow.GetEstimeSize(),		// rowSize in bytes
							numberOfHashBuckets,				// numberOfHashBuckets
							maxInmemoryRowCount,				// maxInmemoryRowCount
							maxInMemorySize,					// maxInMemorySize in bytes
							estimateNumberOfRows,				// estimeRowCount
							-1,									// initialCapacity
							spillLevel
							);
	}
	
	// Incase we are spilling use a different hash
	private int computeBucketNumber(int key) {

		int bucketId;
		int tempHash = key;
		
		for (int i = 0; i <= spillLevel; i++)
		{
			tempHash = hasher_32.hashInt(tempHash).asInt();
			
		}
		bucketId = (tempHash & 0x7fffffff) % numberOfHashBuckets;
		
		return bucketId;
	}
	
	/**
	 * Reads the build side flat file and populate the hash table
	 * @throws ParseException
	 * @throws IOException 
	 */
	public void processBuildSide() throws ParseException, IOException
	{
		int hashBucketID = 0;
		int buildQualifyingRows = 0;
		int buildRowCount = 0;
		List<String> o_rows;
		int o_orderkey;
		Date o_orderdate;
		Date minOrderDate = myFormat.parse(minDateFilter);
		Date maxOrderDate = myFormat.parse(maxDateFilter);
		StringTokenizer st;
		
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
				st = new StringTokenizer(row,",");

				// Parse out orderkey and orderdate
				o_orderkey = Integer.parseInt(st.nextToken());
				o_orderdate = myFormat.parse(st.nextToken());
				
				// hash orderkey and try to figure which hash bucket it should be in
				hashBucketID = computeBucketNumber(o_orderkey);

				// To ensure no skew in hash buckets 
				// Just used for debugging
				++bucketsCount[hashBucketID];
				
				// Apply the filter
				if (o_orderdate.after(minOrderDate) & o_orderdate.before(maxOrderDate))
				{
					// If the row qualifies insert into hash table and bloom filter
					// Gains from bloom filter will only benefit when spilling occurs
					bf.put(o_orderkey);

					spillingHT.insertIntoHashMap(hashBucketID, o_orderkey, new orderkeyRow(o_orderkey, o_orderdate));
					buildQualifyingRows++;
				}

				buildRowCount++;
			
			}
		}
		
		// Capture end time of build phase
		double elapsedTimeInMsecBuildSide = (System.nanoTime() - startBuildPhase) * 1.0e-6;
		
		System.out.println("\n");
		System.out.println("Build : Scanned rows " + buildRowCount );
		System.out.println("Build : Qualified rows "+ buildQualifyingRows);
		System.out.println("Build : Average rows per partition "+ spillingHT.totalnumberOfRows / numberOfHashBuckets);
		System.out.println("Build : Spilled " + spillingHT.GetNumberOfSpilledPartitions() +"/"+numberOfHashBuckets+ " partitions");
		System.out.println("Build : Spilled rows " + spillingHT.GetNumberOfSpilledRows() + ", " +(spillingHT.GetNumberOfSpilledRows() * 100 / buildRowCount ) + "% of scanned rows");
		System.out.println("Build : Spilled rows " + spillingHT.GetNumberOfSpilledRows() + ", " +(spillingHT.GetNumberOfSpilledRows() * 100 / buildQualifyingRows ) + "% of qualified rows");
		System.out.println("Build : Max allowed in-memory rows " + spillingHT.maxInmemoryRowCount + ", " + spillingHT.maxInmemoryRowCount * 100 / buildQualifyingRows + "% of qualified rows");
		System.out.println("Build : Completed in " + elapsedTimeInMsecBuildSide + " msecs, " + buildRowCount / elapsedTimeInMsecBuildSide * 1000 + " rows/sec" + "\n" );

		// Close the files and flush the data
		spillingHT.CloseWriteFiles();
	}
	
	/**
	 * Eventually this function will be 
	 * @return Results set
	 * @throws IOException
	 */
	public ResultSet processProbSideQuickPath() throws IOException
	{
		int l_orderkey;
		Double l_extendedPrice;
		Double summExtendedPrice = 0.0;
		List<String> l_rows;
		long probeRowCount = 0;
		long bloomQualifyingRows = 0;
		long probeSideQualifyingRows = 0;
		int falsePositiveRowCount = 0;
		int hashBucketID;
		StringTokenizer st;
		ResultSet queryResult;
		
		// Capture start time for build side
		long startProbePhase = System.nanoTime(); 
		
		// Now do the probing , just check against bloom filter for now
		while(probeSideFileReader.HasNext())
		{
			l_rows = probeSideFileReader.GetNextBatch();
			for (String row : l_rows)
			{
				st = new StringTokenizer(row,",");
				
				l_orderkey = Integer.parseInt(st.nextToken());
				if (bf.mightContain(l_orderkey))
				{
					bloomQualifyingRows++;
					
					//hashBucketID = (hasher_32.hashInt(l_orderkey).asInt() & 0x7fffffff) % numberOfHashBuckets;
					hashBucketID = computeBucketNumber(l_orderkey);
					
					if (spillingHT.containsKey(l_orderkey, hashBucketID))
					{
						l_extendedPrice = Double.parseDouble(st.nextToken());
						summExtendedPrice += l_extendedPrice;
						probeSideQualifyingRows++;
					}
					// This is only needed for debugging to validate FPR of the Bloom filter
					else
					{
						falsePositiveRowCount++;
					}
				}
				probeRowCount++;
			}
		}

		double elapsedTimeInMsecProbePhase = (System.nanoTime() - startProbePhase) * 1.0e-6;
/*
		System.out.println("Bloom filter size KB " + SizeOf.deepSizeOf(bf) / 1024);
		System.out.println("Hash table size KB " + SizeOf.deepSizeOf(buildSideHashTable) / 1024);
		System.out.println("Hash table2 size KB " + SizeOf.deepSizeOf(spillingHT) / 1024);
*/	
		System.out.println("\n");
		System.out.println("Probe : Scanned " + probeRowCount + " rows");
		System.out.println("Probe : Bloom filter qualified rows " + bloomQualifyingRows);
		System.out.println("Probe : Total qualified rows " + probeSideQualifyingRows);
		System.out.println("Probe : Bloom filter false positives " + falsePositiveRowCount);
		System.out.println("Probe : Bloom filter false positive rate " + ((bloomQualifyingRows - probeSideQualifyingRows ) * 100 / probeSideQualifyingRows));
		System.out.println("Probe : Elapsed time " + elapsedTimeInMsecProbePhase + " msecs, " + probeRowCount / elapsedTimeInMsecProbePhase * 1000 + " rows/sec");
		
		queryResult = new ResultSet(probeSideQualifyingRows, summExtendedPrice);
		
		// Write out the results
		resultSetWriter.writeToFile(queryResult.toString());
		
		return 	queryResult;
	}
	
	/**
	 * Do a collocated join for the spilled partitions
	 * @throws IOException
	 * @throws Exception
	 */
	public void processProbSideSlowPath () throws IOException, Exception
	{
		ResultSet partialResults = new ResultSet(0, 0.0);
		
		// Get the spill files from the build side
		TempFileWriter[] buildSideSpillFileHandler = spillingHT.getBuildSideSpillFileHandlers();
	
		// Hash partition the qualifying rows from the probe side to disk
		// Before spilling the data evaluate the rows against the bloom filter 
		TempFileWriter[] probeSideSpillFileHandler = probeHashTableWithSpilling(probeSideFileReader);
		
		// At this point the in-memory hash partitions are no longer needed and we can release this memory.
		spillingHT.clearInMemoryHashTables();
		
		// Increment the spill level
		spillLevel++;
		
		boolean useRecursion = true;
		
		for (int i =0 ; i < numberOfHashBuckets; i++)
		{
			if (spillingHT.didPartitionSpill(i))
			{
				System.out.println("\n");
				System.out.println("Info : Spill level " + spillLevel);
				System.out.println("Info : Processing spilled partition " + i + " with " + spillingHT.getRowsInPartition(i) + " rows");
				System.out.println("\n");
				
				if(useRecursion)
				{
					// Recursively call the join operator on spilled data, this can spill again if the hash buckets don't fit in memory
					
					HashJoinOperator hashJoinOpOnSpilledData = new HashJoinOperator(
																		this.resultSetPath,
																		probeSideSpillFileHandler[i].getFileName(),
																		buildSideSpillFileHandler[i].getFileName(),
																		spillFilesFolder,
																		numberOfHashBuckets,
																		estimateNumberOfRows,
																		maxInmemoryRowCount,
																		maxInMemorySize,
																		spillLevel,
																		minDateFilter,		
																		maxDateFilter);
					hashJoinOpOnSpilledData.processOp();

				}
				else
				{
					// File handlers for spilled data 
					FileReader spilledBuildSideFileReader = new FileReader(buildSideSpillFileHandler[i].getFileName());
					FileReader spilledProbeSideFileReader = new FileReader(probeSideSpillFileHandler[i].getFileName());
					
					int numberOfSpilledRows = spillingHT.getRowsInPartition(i);
					
					System.out.println("Info : Join spilled partition " + i + " with " + numberOfSpilledRows +" build side rows" + "\n");
					
					// Capture start time for build side
					long startSpilledPartitionJoin = System.nanoTime(); 
					
					// Read the build side spilled data and load it into a hashtable
					HashMap<Integer, orderkeyRow> buildSideHashMap = buildHashMapFromFileReader(spilledBuildSideFileReader ,numberOfSpilledRows);
					
					// Do a collocated join
					partialResults.Add(joinSpilledData(buildSideHashMap, spilledProbeSideFileReader));
					
					resultSetWriter.writeToFile(partialResults.toString());
					
					double elapsedSpilledPartitionJoin = (System.nanoTime() - startSpilledPartitionJoin) * 1.0e-6;
					
					System.out.println("Join spilled partition " + i + " completed in " + elapsedSpilledPartitionJoin + " msecs" + "\n");
				}
			}
		}
	}

	/**
	 * @exception Will join the probe side with the partitions that are in memory 
	 * 			if the partition is not in-memory the row will be spilled to disk
	 * @param fileReader
	 * @return
	 * @throws IOException
	 */
	public TempFileWriter[]  probeHashTableWithSpilling (FileReader fileReader) throws IOException
	{
		int hashBucketID = 0;
		long probeSideFilteredOutRows = 0; // probe side rows that find an in-memory hash table and are filtered out
		long bloomQualifyingRows = 0;
		long probeSideScannedRows = 0;
		long probeSpillRowCount = 0;
		int l_orderkey;
		Double l_extendedPrice;
		Double summExtendedPrice = 0.0;
		long probeSideQualifyingRows = 0;
		List<String> l_rows;
		TempFileWriter[] spillFileHandler = new TempFileWriter[numberOfHashBuckets];
		StringTokenizer st;
		ResultSet queryResult;
		
		// Initialize the files we will spill to
		//probSideSpillFiles = new TempFileWriter[numberOfHashBuckets];
		
		for (int i = 0; i < numberOfHashBuckets; i++)
		{
			spillFileHandler[i] =  new TempFileWriter(spillFilesFolder + "probeSideSpillFile" + i +"." + spillLevel + ".txt", true);
		}
		
		// Capture start time for build side
		long startSPillProbePhase = System.nanoTime(); 
		
		// Now do the probing , just check against bloom filter for now
		while(fileReader.HasNext())
		{
			l_rows = probeSideFileReader.GetNextBatch();
			for (String row : l_rows)
			{
				
				st = new StringTokenizer(row,",");
				
				l_orderkey = Integer.parseInt(st.nextToken());
				
				if (bf.mightContain(l_orderkey))
				{
					// Get the bucket ID
					//hashBucketID = (hasher_32.hashInt(l_orderkey).asInt() & 0x7fffffff) % numberOfHashBuckets;
					hashBucketID = computeBucketNumber(l_orderkey);
					
					l_extendedPrice = Double.parseDouble(st.nextToken());
					
					// If the target partition spilled, write out this file to disk
					if (spillingHT.didPartitionSpill(hashBucketID))
					{
						spillFileHandler[hashBucketID].writeToFile(l_orderkey +","+l_extendedPrice);
						probeSpillRowCount++;
					} 
					else if (spillingHT.containsKey(l_orderkey, hashBucketID))
					{
						// qualify row
						summExtendedPrice += l_extendedPrice;
						probeSideQualifyingRows++;
					}
					else // No match doesn't exist, if bloom filter is sized correctly 1-2% of rows would go through here
					{
						probeSideFilteredOutRows++;
					}
					
					bloomQualifyingRows++;
				}
				probeSideScannedRows++;
			}
		}
		
		// Flush the data and close the file
		for (int i = 0; i < numberOfHashBuckets; i++)
		{
			spillFileHandler[i].CloseFile();
		}
		
		double elapsedTimeInMsecSpillProbePhase = (System.nanoTime() - startSPillProbePhase) * 1.0e-6;
		System.out.println("Probe : Scanned rows " + probeSideScannedRows);
		System.out.println("Probe : Bloom filter qualified rows " + bloomQualifyingRows);
		System.out.println("Probe : Spilled rows " + probeSpillRowCount + ", " +(probeSpillRowCount * 100 / bloomQualifyingRows ) + "% of qualified rows");
		System.out.println("Probe : Rows filtered out via in-memory hash tables " + probeSideFilteredOutRows );
		System.out.println("Probe : Scanned, filtered and spilled in " + elapsedTimeInMsecSpillProbePhase + " msecs, " + probeSideScannedRows / elapsedTimeInMsecSpillProbePhase * 1000 + " rows/sec" + "\n");
		
		queryResult = new ResultSet(probeSideQualifyingRows, summExtendedPrice);
		
		// Write out the results
		resultSetWriter.writeToFile(queryResult.toString());
		
		return spillFileHandler;
	}
	
	/**
	 * @param fr
	 * @param rowCount
	 * @return
	 * @throws ParseException
	 */
	public HashMap<Integer, orderkeyRow> buildHashMapFromFileReader(FileReader fr, int rowCount) throws ParseException
	{
		List<String> o_rows;
		int o_orderkey;
		Date o_orderdate;
		HashMap<Integer, orderkeyRow> buildSideHashMap = new HashMap<Integer, orderkeyRow>(rowCount);
		StringTokenizer st ;
		
		// Capture start time for build side
		long startBuildPhase = System.nanoTime(); 
		
		while(fr.HasNext())
		{
			// Get a batch of rows
			o_rows = fr.GetNextBatch();
			
			// Now Hash o_orderkey
			for (String row : o_rows) {

				// Split the row to get the columns
				//columns = row.split(",");
				st = new StringTokenizer(row,",");
				
				// Parse out orderkey and orderdate
				o_orderkey = Integer.parseInt(st.nextToken());
				o_orderdate = myFormat.parse(st.nextToken());

				// Insert the key value pair into the hash map
				buildSideHashMap.put(o_orderkey, new orderkeyRow(o_orderkey, o_orderdate));
			}
		}
		
		double elapsedTimeInMsecBuildProbePhase = (System.nanoTime() - startBuildPhase) * 1.0e-6;
		
		System.out.println("	Build spilled partition scanned rows " + rowCount);
		System.out.println("	Build spilled partition hash map build completed in  " + elapsedTimeInMsecBuildProbePhase + " msecs , " +(rowCount / elapsedTimeInMsecBuildProbePhase * 1000) + " rows/sec" + "\n");
		
		return buildSideHashMap;
	}
	
	/**
	 * @param buildSideHashMap
	 * @param spilledProbeSideFileReader
	 * @return
	 * @throws Exception 
	 */
	public ResultSet joinSpilledData(HashMap<Integer, orderkeyRow> buildSideHashMap, FileReader spilledProbeSideFileReader) throws Exception
	{
		ResultSet partialResultSet = null;
		int l_orderkey;
		Double l_extendedPrice;
		Double summExtendedPrice = 0.0;
		List<String> l_rows;
		int qualifyingRowCount = 0;
		int filteredRowCount = 0;
		int scannedRowCount = 0;
		StringTokenizer st;


		// Capture start time for build side
		long startProbePhase = System.nanoTime(); 
		
		while(spilledProbeSideFileReader.HasNext())
		{
			l_rows = spilledProbeSideFileReader.GetNextBatch();
			for (String row : l_rows)
			{
				//columns = row.split(",");
				st = new StringTokenizer(row,",");
				l_orderkey = Integer.parseInt(st.nextToken());
				//l_orderkey = Integer.parseInt(columns[0]);
				
				if (buildSideHashMap.containsKey(l_orderkey))
				{
					l_extendedPrice = Double.parseDouble(st.nextToken());
					summExtendedPrice += l_extendedPrice;
					qualifyingRowCount++;
				}
				else
				{
					filteredRowCount++;
				}
				scannedRowCount++;
			}
		}
		
		double elapsedTimeInMsecSpillProbePhase = (System.nanoTime() - startProbePhase) * 1.0e-6;
		
		System.out.println("	Probe spilled partition scanned rows " + scannedRowCount);
		System.out.println("	Probe spilled partition qualified rows " + qualifyingRowCount);
		System.out.println("	Probe spilled partition filterd out rows " + filteredRowCount);
		System.out.println("	Probe spilled partition completed in  " + elapsedTimeInMsecSpillProbePhase + ", " +(scannedRowCount / elapsedTimeInMsecSpillProbePhase * 1000) + " rows/sec" + "\n");
		
		// Write out the intermediate results to the out file
		resultSetWriter.writeToFile(qualifyingRowCount+","+summExtendedPrice);
		
		partialResultSet = new ResultSet(qualifyingRowCount, summExtendedPrice);
		
		return partialResultSet;
	}
	
	public void processOp() throws Exception {
		
		// Capture start time for build side
		long startHashJoin = System.nanoTime(); 
		
		// Read data from probe side and insert into N hash tables
		processBuildSide();
		
		if (!spillingHT.hasSpilledData())
		{
			processProbSideQuickPath();
		}
		else
		{
			processProbSideSlowPath();
		}
 
		// Current query is a scalar so can read out the results and do the 
		// final aggregation
		resultSetWriter.CloseFile();
		
		// Check if we have any skew
		spillingHT.dumpRowsInHashBuckets();
		
		// Avoid leak handles
		probeSideFileReader.closefile();
		buildSideFileReader.closefile();
		
		double elapsedTimeInMsecHashJoin = (System.nanoTime() - startHashJoin) * 1.0e-6;
		System.out.println("Hash join with spill level " + spillLevel + " completed in " + elapsedTimeInMsecHashJoin +" msecs" );

	}
}
