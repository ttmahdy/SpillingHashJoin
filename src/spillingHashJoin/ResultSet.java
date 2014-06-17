package spillingHashJoin;

public class ResultSet {

	@Override
	public String toString() {
		return rowCount + "," + sumExtendedPrice;
	}
	public ResultSet(long rowCount, Double sumExtendedPrice) {
		super();
		this.rowCount = rowCount;
		this.sumExtendedPrice = sumExtendedPrice;
	}
	
	public void Add(ResultSet rs)
	{
		this.rowCount += rs.rowCount;
		this.sumExtendedPrice += rs.sumExtendedPrice;
	}
	
	public long rowCount;
	public Double sumExtendedPrice;
}
