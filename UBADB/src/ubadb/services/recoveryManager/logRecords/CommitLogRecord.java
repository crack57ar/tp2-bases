package ubadb.services.recoveryManager.logRecords;

public class CommitLogRecord extends LogRecord
{
	public static final byte COMMIT = 1;
	private int transactionId;

	public CommitLogRecord(int transactionId)
	{
		this.transactionId = transactionId;
	}

	public int getTransactionId()
	{
		return transactionId;
	}
}
