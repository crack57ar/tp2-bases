package ubadb.services.recoveryManager.logRecords;

import ubadb.common.PageIdentifier;

public class UpdateLogRecord extends LogRecord
{
	private int 			transactionId;
	private PageIdentifier 	pageId;
	private short 			length;
	private short 			offset;
	private byte[]			beforeImage;
	private byte[]			afterImage;

	public UpdateLogRecord(int transactionId, PageIdentifier pageId, short length, short offset, byte[] beforeImage, byte[] afterImage)
	{
		this.transactionId 	= transactionId;
		this.pageId 		= pageId;
		this.length 		= length;
		this.offset 		= offset;
		this.beforeImage 	= beforeImage;
		this.afterImage 	= afterImage;
	}

	public int getTransactionId()
	{
		return transactionId;
	}

	public PageIdentifier getPageId()
	{
		return pageId;
	}

	public short getLength()
	{
		return length;
	}

	public short getOffset()
	{
		return offset;
	}

	public byte[] getBeforeImage()
	{
		return beforeImage;
	}

	public byte[] getAfterImage()
	{
		return afterImage;
	}
}