Grace Hash join implementation in Java, the implementation supports spilling when the memory limit is reached and utilizes bloom filter to reduce the number of data spilled from the probe side.
The app uses TPCH Flat files to simulate the following join :
	select count(*), sum(L_EXTENDEDPRICE) from ORDERS, LINEITEM  
	where L_ORDERKEY = O_ORDERKEY
 	and O_ORDERDATE between 1995-01-02 and 1996-01-02 
 
 
