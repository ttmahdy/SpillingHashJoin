package spillingHashJoin;

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
		String probeSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//30G//narrowLineitem30G.tbl"; 
		String buildSideFilePath = "//Users//mmokhtar//Downloads//FlatFiles//30G//narrowOrders30G.tbl";
		
		// Path for results file
		String resultSetFilePath = "//Users//mmokhtar//Downloads//spillData//queryResults.csv";

		// Path for spill files
		String spillFilesFolder = "//Users//mmokhtar//Downloads//spillData//";

		// Filter on o_orderdate
		String minDateFilter = "1994-01-02";
		String maxDateFilter = "1995-01-02";
		
		// Info for Hash table
		int numberOfHashBuckets 	= 8;				// This is the unit of Spilling
		int estimateNumberOfRows 	= 5000000;			// CE is 10e6 for build size use to construct the bloom filter
		int maxInmemoryRowCount  	= 2000000;	 		// 2500000 no spill 1994-01-02 and 1995-01-02 
		int spillLevel 				= 0;				// Always start at 0, first time we spill level is 1, next time 2 etc..
		
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
