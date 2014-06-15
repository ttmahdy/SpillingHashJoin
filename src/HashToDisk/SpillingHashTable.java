package HashToDisk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.Charsets;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class SpillingHashTable<T1, T2> {

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
			int initialCapacity) throws IOException 
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
		currentlySpilling = false;
		
		// We could add more control by keeping all hash buckets under control
		// incases there is data skew or such this can happen and as a result 
		// other hash buckets will be victimized.
		maxPerBucketRowCount = maxInmemoryRowCount / numberOfHashBuckets;
		
		// Used as a threshold to invoke spilling
		estimateMaxRowsThatCanFit = maxInMemorySize / rowSize;
		
		currentNumberOfRows = 0;
		bucketsCount = new int[numberOfHashBuckets];
		buildSideSpillFiles = new SpillFileHandler[numberOfHashBuckets];
		spilledPartitions = new boolean[numberOfHashBuckets];

		// Bloom filters have relatively small size compared, if the estimate is way off
		// they become ineffective with very high false positive rate
		// So compensate for the possibility of having low cardinality
		bf = BloomFilter.create(Funnels.integerFunnel(), estimateRowCount * 5);
		
		// Allocate the memory for the list of Hash maps
		hashMapList = new ArrayList<HashMap<T1,T2>>();
		for (int i = 0 ; i < numberOfHashBuckets; i ++)
		{
			hashMapList.add(new HashMap<T1, T2>());
	        // use passed in capacity and loadfactor if not -1, you must specify
	        // capacity if you want to specify loadfactor.
	        if (initialCapacity != -1)
	        {
	        	hashMapList.add(new HashMap<T1, T2>(initialCapacity));
	        }
	        else
	        {
	        	// If the estimate is an overly big number we can hit OOM right away
	        	hashMapList.add(new HashMap<T1, T2>(Math.min(estimateRowCount,maxInmemoryRowCount)));
	        }
	        
	        bucketsCount[i] = 0;
	        spilledPartitions[i] = false;
	        buildSideSpillFiles[i] =  new SpillFileHandler(spillFilesFolder + "buildSideSpillFile" + i + ".txt");
		}
	}
	
	public HashMap<T1, T2> GetHashMapAt(int position)
	{
		return hashMapList.get(position);
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
	public void InsertIntoHashMap(int hashBucketID, T1 key, T2 value)
	{
		if (ShouldSpill())
		{
			try {
				SpillHashTable(FindHashTableToSpill());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (spilledPartitions[hashBucketID])
		{
			SpillRow(value, hashBucketID);
		}
		else
		{
			hashMapList.get(hashBucketID).put(key, value);
			bucketsCount[hashBucketID]++;
			currentNumberOfRows++;
		}
	}
	
	public boolean containsKey(T1 key) {
		HashCode hc = hasher_32.hashString(key.toString(), Charsets.UTF_8);
		int hcInt = hc.asInt();
		int hashBucketID = (hcInt & 0x7fffffff) % numberOfHashBuckets;
		return hashMapList.get(hashBucketID).containsKey(key);
    }
	
	public boolean containsKey(T1 key, int hashBucketID ) {
		
		// We shouldn't be trying to access a spilled partition
		assert (spilledPartitions[hashBucketID] == false);
		
		return hashMapList.get(hashBucketID).containsKey(key);
    }

	public T2 getKey(T1 key) {
		HashCode hc = hasher_32.hashString(key.toString(), Charsets.UTF_8);
		int hcInt = hc.asInt();
		int hashBucketID = (hcInt & 0x7fffffff) % numberOfHashBuckets;
		return hashMapList.get(hashBucketID).get(key);
    }
	
	public void DumpRowsInHashBuckets() {
		for (int i = 0 ; i < numberOfHashBuckets; i ++)
		{
			System.out.println("Has bucket " + i + " has " + hashMapList.get(i).size() + " rows" );
		}
	}
	
	/**
	 * Checks if we should spill a partition or not
	 * @return
	 */
	public boolean ShouldSpill()
	{
		if ( (currentNumberOfRows >= maxInmemoryRowCount) ||  			// if we exceeded the in-memory row count
			 (currentNumberOfRows >= estimateMaxRowsThatCanFit))  		// if we exceeded the memory quota
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
	
	/**
	 * @param hashBucketId
	 * @return
	 */
	public boolean DidPartitionSpill(int hashBucketId)
	{
		return spilledPartitions[hashBucketId];
	}
	
	public boolean HasSpilledData()
	{
		return hasSpilledData;
	}

	public SpillFileHandler[] GetBuildSideSpillFileHandlers()
	{
		return buildSideSpillFiles;
	}
	
	/**
	 * @param value
	 * @param bucketID
	 * Spills a row to a file
	 */
	public void SpillRow(T2 value, int bucketID) {
			try {
				buildSideSpillFiles[bucketID].writeToFile(value.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public void SpillHashTable(int bucketID) throws IOException {
		for (T2 row :  hashMapList.get(bucketID).values())
		{
			buildSideSpillFiles[bucketID].writeToFile(row.toString());
		}
		
		currentNumberOfRows = currentNumberOfRows - hashMapList.get(bucketID).size(); 
		
		// We should not end up with negative number of rows or there is an overflow or a bug
		assert(currentNumberOfRows > 0);
		
		// Change the state to reflect that we aren't spilling any more
		// this can change if we are reach the memory limits again
		currentlySpilling = false;
		
		// Clear all the contents of the hash table
		hashMapList.get(bucketID).clear();
	}
	
	public int FindHashTableToSpill ()
	{
		int bucketID = -1;
		int maxRowCount = 0;
		
		for (int i = 0; i < numberOfHashBuckets; i++)
		{
			if(bucketsCount[i] > maxRowCount && spilledPartitions[i] != true)
			{
				maxRowCount = bucketsCount[i];
				bucketID = i;
			}
		}
		
		// if we end up with -1 this means we couldn't find any partitions to spill and 
		// the application will hit OOM very soon
		assert(bucketID >= 0);
		
		// Mark the partition as spilled
		spilledPartitions[bucketID] = true;
		
		return bucketID;
	}
	
	public
	String spillFilesFolder;
	int currentNumberOfRows;
	int rowSize;
	int estimateRowCount;
	int numberOfHashBuckets;
	int maxInmemoryRowCount;
	int maxPerBucketRowCount;
	int maxInMemorySize;
	int estimateMaxRowsThatCanFit;
	boolean currentlySpilling;
	boolean hasSpilledData = false;
	HashFunction hasher_32 = Hashing.murmur3_32();
	int[] bucketsCount ;
	SpillFileHandler[] buildSideSpillFiles;
	
	// Bloom filter
	BloomFilter<Integer> bf;
	
	// Consider not building the bloom filter again for spilled data
	// as the filter has already been applied, so most likely it won't 
	// provide much benefit
	boolean buildBloomfilter;
	
	boolean[] spilledPartitions;
	List<HashMap<T1, T2>> hashMapList;
	
}
