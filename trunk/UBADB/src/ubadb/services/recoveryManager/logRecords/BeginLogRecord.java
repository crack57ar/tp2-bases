package ubadb.services.recoveryManager.logRecords;

public class BeginLogRecord extends LogRecord
{
	public static final byte BEGIN = 0;
	private int transactionId;

	public BeginLogRecord(int transactionId)
	{
		this.transactionId = transactionId;
	}

	public int getTransactionId()
	{
		return transactionId;
	}
}
