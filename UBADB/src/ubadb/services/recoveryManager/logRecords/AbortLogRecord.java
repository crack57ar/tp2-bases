package ubadb.services.recoveryManager.logRecords;

public class AbortLogRecord extends LogRecord
{
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
