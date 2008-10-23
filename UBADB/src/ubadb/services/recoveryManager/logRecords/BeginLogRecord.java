package ubadb.services.recoveryManager.logRecords;

public class BeginLogRecord extends LogRecord
{
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
