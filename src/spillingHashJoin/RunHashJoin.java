package spillingHashJoin;

import java.io.File;
import java.io.FileNotFoundException;

public abstract class RunHashJoin {

	public static void main(String[] args) throws Exception {

		/*
		// TODO Auto-generated method stub
		// 1GB TPCH
		//String probeSideFilePath = "//Users//mmokhtar//Downloads//tpch_2_17_0//dbgen//TPCH//lineitem.tbl";
		//String buildSideFilePath = "//Users//mmokhtar//Downloads//tpch_2_17_0//dbgen//TPCH//orders.tbl";

		// 10GB TPCH
		//String probeSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//lineitem.tbl";
		//String buildSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//orders.tbl";

		// 1GB TPCH row count but narrow flat files
		// Orders 	has 1,500,000 rows
		// Lineitem has 6,001,215 rows
		//String probeSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//narrowLineitem1G.tbl";
		//String buildSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//narrowOrders1G.tbl";
		*/
		
		// 30GB TPCH row count but narrow flat files
		// Orders 	has 45,000,000 	rows
		// Lineitem has 179,958,156	rows
		
		String probeSideFilePath;
		String buildSideFilePath;
		String resultSetFilePath;
		String spillFilesFolder;
		
		// Info for Hash table
		int spillLevel = 0;
		int numberOfHashBuckets 	= 8;				// This is the unit of Spilling
		int estimateNumberOfRows 	= 5000000;			// CE is 10e6 for build size use to construct the bloom filter
		int maxInmemoryRowCount  	= 2000000;	 		// 2500000 no spill 1994-01-02 and 1995-01-02 

		
		if (args.length >= 3)
		{
			// Parse the args
			buildSideFilePath = args[0];
			probeSideFilePath = args[1];
			spillFilesFolder  = args[2];
			resultSetFilePath = args[3];
		}
		else
		{
			buildSideFilePath 	= "//Users//mmokhtar//Downloads//FlatFiles//30G//narrowOrders30G.tbl";
			probeSideFilePath 	= "//Users//mmokhtar//Downloads//FlatFiles//30G//narrowLineitem30G.tbl";
			spillFilesFolder 	= "//Users//mmokhtar//Downloads//spillData//";
			resultSetFilePath 	= "//Users//mmokhtar//Downloads//spillData//queryResults.csv";

		}

		if (args.length ==6)
		{
			numberOfHashBuckets		= Integer.parseInt(args[4]);
			estimateNumberOfRows	= Integer.parseInt(args[5]);
			maxInmemoryRowCount		= Integer.parseInt(args[6]);
		}
		else
		{
			numberOfHashBuckets 	= 8;				// This is the unit of Spilling
			estimateNumberOfRows 	= 5000000;			// CE is 10e6 for build size use to construct the bloom filter
			maxInmemoryRowCount  	= 2000000;	 		// 2500000 no spill 1994-01-02 and 1995-01-02 
		}
		
		File buildSideFile = new File(buildSideFilePath);
		File probeSideFile = new File(probeSideFilePath);
		
		if (!buildSideFile.exists() || !probeSideFile.exists())
		{
			System.out.println("Usage : SpillingHashTable.jar probeSideFileName probeSideFileName spillFilesFolder resultSetFileName numberOfHashBuckets estimateNumberOfRows");
			throw new FileNotFoundException("Files " + buildSideFilePath + " & " + probeSideFilePath + " not found!");
		}
		
		// Filter on o_orderdate
		String minDateFilter = "1994-01-02";
		String maxDateFilter = "1995-01-02";
		
		 
		// TODO need to handle this correctly by keeping the hash tables size in check
		int maxInMemorySize = Integer.MAX_VALUE; 	
		
		HashJoinOperator hashJoinOp = new HashJoinOperator(	
										resultSetFilePath,
										probeSideFilePath,
										buildSideFilePath,
										spillFilesFolder,
										numberOfHashBuckets,
										estimateNumberOfRows,
										maxInmemoryRowCount,
										maxInMemorySize,
										spillLevel,
										minDateFilter,
										maxDateFilter);
		
		hashJoinOp.processOp();
	}

}
