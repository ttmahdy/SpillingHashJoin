package spillingHashJoin;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.Charsets;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class SpillingHashTable<T1, T2> {
	public
	String spillFilesFolder;
	int totalnumberOfRows;
	int numberOfInMemRows;
	int numberOfSpilledRows;
	int rowSize;
	int estimateRowCount;
	int numberOfHashBuckets;
	int maxInmemoryRowCount;
	int maxPerBucketRowCount;
	int maxInMemorySize;
	int spillLevel;
	int estimateMaxRowsThatCanFit;
	boolean currentlySpilling;
	boolean hasSpilledData = false;
	HashFunction hasher_32 = Hashing.murmur3_32();
	int[] bucketsCount;
	int[] spillCounts;
	TempFileWriter[] buildSideSpillFiles;
	
	// Bloom filter
	BloomFilter<Integer> bf;
	
	// Consider not building the bloom filter again for spilled data
	// as the filter has already been applied, so most likely it won't 
	// provide much benefit
	boolean buildBloomfilter;
	
	boolean[] spilledPartitions;
	HashMap<Integer, HashMapContainer<T1, T2>> bucketHashMaps;
	
	@SuppressWarnings("hiding")
	private class HashMapContainer<T1, T2> extends HashMap<T1, T2> {
		private static final long serialVersionUID = 1L;

		public HashMapContainer(int initialSize,
				int partitionId) {
			super(initialSize);
			this.partitionId = partitionId;
		}
		// TODO remove this or add more useful info
		@SuppressWarnings("unused")
		int partitionId;
	}
	
	/**
	 * @param rowSize
	 * @param numberOfHashBuckets
	 * @param maxInmemoryRowCount
	 * @param maxInMemorySize
	 * @param estimateRowCount
	 * @param initialCapacity
	 * @throws IOException 
	 */
	public SpillingHashTable(
			String spillFilesFolder,
			int rowSize,
			int numberOfHashBuckets, 
			int maxInmemoryRowCount,
			int maxInMemorySize, 
			int estimateRowCount,
			int initialCapacity,
			int spillLevel) throws IOException 
	{
		super();
		assert(maxInmemoryRowCount > 1);
		assert(numberOfHashBuckets > 1);
		assert(maxInMemorySize > 1);
		assert(spillFilesFolder != null);
		
		this.spillFilesFolder = spillFilesFolder;
		this.rowSize = rowSize;
		this.estimateRowCount = estimateRowCount;
		this.numberOfHashBuckets = numberOfHashBuckets;
		this.maxInmemoryRowCount = maxInmemoryRowCount;
		this.maxInMemorySize = maxInMemorySize;
		this.spillLevel = spillLevel;
		currentlySpilling = false;
		
		// We could add more control by keeping all hash buckets under control
		// incases there is data skew or such this can happen and as a result 
		// other hash buckets will be victimized.
		maxPerBucketRowCount = maxInmemoryRowCount / numberOfHashBuckets;
		
		// Used as a threshold to invoke spilling
		estimateMaxRowsThatCanFit = maxInMemorySize / rowSize;
		
		numberOfInMemRows = 0;
		numberOfSpilledRows = 0;
		totalnumberOfRows = 0;
		bucketsCount = new int[numberOfHashBuckets];
		spillCounts = new int[numberOfHashBuckets];
		buildSideSpillFiles = new TempFileWriter[numberOfHashBuckets];
		spilledPartitions = new boolean[numberOfHashBuckets];

		// Bloom filters have relatively small size compared, if the estimate is way off
		// they become ineffective with very high false positive rate
		// So compensate for the possibility of having low cardinality
		bf = BloomFilter.create(Funnels.integerFunnel(), estimateRowCount * 5);
		
		// Allocate the memory for the list of Hash maps
		bucketHashMaps = new HashMap<Integer,HashMapContainer<T1,T2>>(numberOfHashBuckets);
		
		for (int i = 0 ; i < numberOfHashBuckets; i ++)
		{
	        // use passed in capacity and loadfactor if not -1, you must specify
	        // capacity if you want to specify loadfactor.
	        if (initialCapacity != -1)
	        {
	        	bucketHashMaps.put(i, new HashMapContainer<T1, T2>(initialCapacity, i));
	        }
	        else
	        {
	        	// If the estimate is an overly big number we can hit OOM right away
	        	int initialSize = Math.min(estimateRowCount,maxInmemoryRowCount) / numberOfHashBuckets;
	        	bucketHashMaps.put(i, new HashMapContainer<T1, T2>(initialSize, i));
	        }
	        
	        bucketsCount[i] = 0;
	        spilledPartitions[i] = false;
	        buildSideSpillFiles[i] =  new TempFileWriter(spillFilesFolder + "buildSideSpillFile" + i + "." + spillLevel + ".txt", true);
		}
	}
	
	public HashMap<T1, T2> getHashMapAt(int position)
	{
		return bucketHashMaps.get(position);
	}
	
	
	/**
	 * @param key
	 * @param value
	 * Try to insert a row into the Hashtable
	 * 	1) Find which hash bucket the key belongs to
	 * 	2) Check if we need to spill due to reaching memory limits, spill a partition if true, we should atleast have enough memory
	 *     for one partition
	 *  3) Try to write-out the row, if the target partition is spilled write to disk else insert into the in-memory hash table   
	 * 
	 */
	public void insertIntoHashMap(int hashBucketID, T1 key, T2 value)
	{
		// If we should spill due to the last inserted row 
		// find the largest in-memory partition and write out the contents
		// to disk
		if (shouldSpill())
		{
			try {
				spillHashTable(findHashTableToSpill());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (spilledPartitions[hashBucketID])
		{
			spillRow(value, hashBucketID);
			spillCounts[hashBucketID]++;
		}
		else
		{
			bucketHashMaps.get(hashBucketID).put(key, value);
			numberOfInMemRows++;
		}
		
		totalnumberOfRows++;
		bucketsCount[hashBucketID] ++;
		
	}
	
	public boolean containsKey(T1 key) {
		HashCode hc = hasher_32.hashString(key.toString(), Charsets.UTF_8);
		int hcInt = hc.asInt();
		int hashBucketID = (hcInt & 0x7fffffff) % numberOfHashBuckets;

		return bucketHashMaps.get(hashBucketID).containsKey(key);
    }
	
	public boolean containsKey(T1 key, int hashBucketID ) {
		
		// We shouldn't be trying to access a spilled partition
		assert (spilledPartitions[hashBucketID] == false);

		return bucketHashMaps.get(hashBucketID).containsKey(key);
    }

	/**
	 * Not used for now as current query only checks exists 
	 * @param key
	 * @return
	 */
	public T2 getKey(T1 key) {
		HashCode hc = hasher_32.hashString(key.toString(), Charsets.UTF_8);
		int hcInt = hc.asInt();
		int hashBucketID = (hcInt & 0x7fffffff) % numberOfHashBuckets;
	
		return bucketHashMaps.get(hashBucketID).get(key);
    }
	
	
	/**
	 * Dump the number of rows per hash bucket
	 * If there is skew the hashing function needs to modified
	 */
	public void dumpRowsInHashBuckets() {
		
		int numberOfKeys;
		
		System.out.println("\n" + "Hash bucket info for Spill level " + spillLevel);
		for (int i = 0 ; i < numberOfHashBuckets; i ++)
		{
			numberOfKeys = bucketHashMaps.containsKey(i) ? bucketHashMaps.get(i).size() : 0; 
			System.out.println("	Bucket " + i + " has " + bucketsCount[i] + " rows,  " + numberOfKeys + " keys" );
		}
	}
	
	/**
	 * Checks if we reached the in-memory row count limit if so we should start spilling
	 * @return
	 */
	public boolean shouldSpill()
	{
		if ( (numberOfInMemRows >= maxInmemoryRowCount) ||  			// if we exceeded the in-memory row count
			 (numberOfInMemRows >= estimateMaxRowsThatCanFit))  		// if we exceeded the memory quota
		{
			// Change the state to reflect that we are now spilling
			currentlySpilling = true;
			hasSpilledData = true;
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public int getRowsInPartition(int hashBucketId)
	{
		return bucketsCount[hashBucketId];
	}
	
	/**
	 * @param hashBucketId
	 * @return
	 */
	public boolean didPartitionSpill(int hashBucketId)
	{
		return spilledPartitions[hashBucketId];
	}
	
	public boolean hasSpilledData()
	{
		return hasSpilledData;
	}

	public TempFileWriter[] getBuildSideSpillFileHandlers()
	{
		return buildSideSpillFiles;
	}
	
	public int GetNumberOfSpilledRows()
	{
		return numberOfSpilledRows;
	}
	
	/**
	 * @param value
	 * @param bucketID
	 * Spills a row to a file
	 */
	public void spillRow(T2 value, int bucketID) {
			try {
				buildSideSpillFiles[bucketID].writeToFile(value.toString());
				numberOfSpilledRows++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public void spillHashTable(int bucketID) throws IOException {
		
		// Mark the partition as spilled
		spilledPartitions[bucketID] = true;

		int spilledRows = 0;
		
		for (T2 row :  bucketHashMaps.get(bucketID).values())
		{
			buildSideSpillFiles[bucketID].writeToFile(row.toString());
			spilledRows++;
		}
		
		int bucketRowCount = bucketsCount[bucketID];
		spillCounts[bucketID] += spilledRows;

		numberOfSpilledRows += bucketRowCount;
		
		numberOfInMemRows = numberOfInMemRows - bucketRowCount;
		
		// We should not end up with negative number of rows or there is an overflow or a bug
		assert(numberOfInMemRows > 0);
		
		// Change the state to reflect that we aren't spilling any more
		// this can change if we are reach the memory limits again
		currentlySpilling = false;
		
		// Clear the reference to the hash map so that it gets GCied
		bucketHashMaps.remove(bucketID);
	}
	
	/**
	 * Delete refs so unwanted in-memory hash maps get garbage collected
	 */
	public void clearInMemoryHashTables()
	{

		 int mb = 1024*1024;
		 boolean issueGC = false;

		 //Getting the runtime reference from system
	     Runtime runtime = Runtime.getRuntime();
	         
	     System.out.println("Info : Delete referance for in-memory hash tables as they are no longer needed");
	         
	     long freeMemoryBeforeRefDelete = runtime.freeMemory();
	     
	     //Print used memory
	     System.out.println("	Memory : Used MB before reference delete "
	            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
	     
	    for (int i = 0; i < numberOfHashBuckets; i++)
		{
			if (!spilledPartitions[i])
			{
				// Clear the reference to the hash map so that it gets GCied
				bucketHashMaps.remove(i);
				issueGC = true;
			}
		}
	     
	     if(issueGC)
	     {
		    // Invoke GC if we should 
		    runtime.gc();
	     }
	    
	     long freeMemoryAfterRefDelete = runtime.freeMemory();
	    
	    //Print used memory
	     System.out.println("	Memory : Freed MB "
	            + (freeMemoryAfterRefDelete - freeMemoryBeforeRefDelete) / mb);

	     //Print used memory
	     System.out.println("	Memory : Used after referance delete "
	            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
	}
	
	/**
	 * Find the biggest in-memory hash table and spill it
	 * Ensure that we can fit a couple of partitions in-memory and 
	 * not spill everything.  
	 * @return hash bucket ID for the partition to spill 
	 */
	public int findHashTableToSpill()
	{
		int bucketID = -1;
		int maxRowCount = 0;
		
		for (int i = 0; i < numberOfHashBuckets; i++)
		{
			// Use the row count and not the HashMap.size as it returns the number of keys
			if(bucketsCount[i] > maxRowCount && spilledPartitions[i] != true)
			{
				maxRowCount = bucketsCount[i];
				bucketID = i;
			}
		}
		
		// if we end up with -1 this means we couldn't find any partitions to spill and 
		// the application will hit OOM very soon
		assert(bucketID >= 0);
		
		return bucketID;
	}
	
	public void CloseWriteFiles() throws IOException
	{
		for (int i = 0; i < numberOfHashBuckets; i++)
		{
			buildSideSpillFiles[i].CloseFile();
		}
	}
	
	public int GetNumberOfSpilledPartitions()
	{
		int numberOfSpilledPartitions = 0;
		for (int i = 0; i < numberOfHashBuckets; i++)
		{
			if (spilledPartitions[i])
			{
				numberOfSpilledPartitions++;
			}
		}
		
		return numberOfSpilledPartitions;
	}
}
