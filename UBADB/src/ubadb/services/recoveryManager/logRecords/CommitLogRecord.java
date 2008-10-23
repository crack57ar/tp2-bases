package ubadb.services.recoveryManager.logRecords;

public class CommitLogRecord extends LogRecord
{
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
