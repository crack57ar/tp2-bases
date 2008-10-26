package ubadb.services.recoveryManager.logRecords;



public class AbortLogRecord extends LogRecord
{
	public static final byte ABORT = 2;
	private int transactionId;

	public AbortLogRecord(int transactionId)
	{
		this.transactionId = transactionId;
	}

	public int getTransactionId()
	{
		return transactionId;
	}
}
