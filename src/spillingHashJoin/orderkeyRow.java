package spillingHashJoin;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

// Define the row that will be stored in the hash table
public class orderkeyRow implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	public String toString() {
		return  o_orderkey + ","+ myFormat.format(o_orderdate);
	}

	public orderkeyRow(int o_orderkey, Date o_orderdate) {
		super();
		this.o_orderkey = o_orderkey;
		this.o_orderdate = o_orderdate;
	}
	
	public static int GetEstimeSize()
	{
		return Integer.SIZE / 8 + 32 /*java.util.Date is 32 bytes*/ ;
	}
	
	public int o_orderkey;
	public Date o_orderdate;
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((o_orderdate == null) ? 0 : o_orderdate.hashCode());
		result = prime * result + o_orderkey;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		orderkeyRow other = (orderkeyRow) obj;

		if (o_orderdate == null) {
			if (other.o_orderdate != null)
				return false;
		} else if (!o_orderdate.equals(other.o_orderdate))
			return false;
		if (o_orderkey != other.o_orderkey)
			return false;
		return true;
	}

}