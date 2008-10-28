package ubadb.test.logGenerator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import sun.reflect.generics.tree.ReturnType;
import ubadb.common.PageIdentifier;
import ubadb.logger.DBLogger;
import ubadb.services.recoveryManager.exceptions.RecoveryManagerException;
import ubadb.services.recoveryManager.logRecords.AbortLogRecord;
import ubadb.services.recoveryManager.logRecords.BeginLogRecord;
import ubadb.services.recoveryManager.logRecords.CommitLogRecord;
import ubadb.services.recoveryManager.logRecords.LogRecord;
import ubadb.services.recoveryManager.logRecords.UpdateLogRecord;

/**
 *	Sirve para generar logs de prueba 
 */
public class RecoveryLogGenerator
{
	
	
	public static void main(String[] args)
	{
		String outputLog = "out/RecoveryManagerLog.dat";
		
		List<LogRecord> records;
		//records = generateRecords();
		
		//saveToFile(records, outputLog);
		
		records = getFromFile(outputLog);
		
	}

	private static List<LogRecord> generateRecords()
	{
		List<LogRecord> ret = new ArrayList<LogRecord>();
		
		//Acá va lo que uno quiere generar
		
		ret.add(new BeginLogRecord(1));
		ret.add(new BeginLogRecord(2));
		ret.add(new BeginLogRecord(3));
		ret.add(new BeginLogRecord(4));
		ret.add(new BeginLogRecord(5));
		ret.add(new BeginLogRecord(6));
		ret.add(new BeginLogRecord(7));//incomplete
		
		ret.add(new UpdateLogRecord(1,new PageIdentifier(10,20),(short)2,(short)0,new byte[]{0,0},new byte[]{1,1}));
		ret.add(new UpdateLogRecord(6,new PageIdentifier(11,20),(short)2,(short)0,new byte[]{0,0},new byte[]{1,1}));// incomplete
		ret.add(new UpdateLogRecord(3,new PageIdentifier(12,20),(short)2,(short)0,new byte[]{0,0},new byte[]{1,2}));// incomplete
		ret.add(new UpdateLogRecord(4,new PageIdentifier(13,20),(short)2,(short)0,new byte[]{0,0},new byte[]{4,5}));
		
		
		ret.add(new CommitLogRecord(1));
		ret.add(new AbortLogRecord(2));
		
		ret.add(new AbortLogRecord(4));
		ret.add(new AbortLogRecord(5));
		ret.add(new BeginLogRecord(8));
		ret.add(new BeginLogRecord(9));
		ret.add(new BeginLogRecord(10));
		ret.add(new BeginLogRecord(11));
		ret.add(new BeginLogRecord(12));
		
		ret.add(new UpdateLogRecord(10,new PageIdentifier(10,20),(short)2,(short)0,new byte[]{0,0},new byte[]{1,1}));
		ret.add(new UpdateLogRecord(10,new PageIdentifier(11,20),(short)2,(short)0,new byte[]{0,0},new byte[]{1,1}));
		ret.add(new UpdateLogRecord(10,new PageIdentifier(12,20),(short)2,(short)0,new byte[]{0,0},new byte[]{1,2}));
		ret.add(new UpdateLogRecord(10,new PageIdentifier(13,20),(short)2,(short)0,new byte[]{0,0},new byte[]{4,5}));
		
		ret.add(new CommitLogRecord(10));
		
		
		return ret;
	}

	private static void saveToFile(List<LogRecord> records, String outputLog)
	{
		//Escribo el arreglo de bytes de cada record en el archivo de salida
		new File(outputLog).delete();
		try {
			DataOutputStream stream = new DataOutputStream(new FileOutputStream(outputLog));
			 for(LogRecord record : records){
				serialize(record, stream);
			}
			stream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static List<LogRecord> getFromFile(String input) {
		File infile = new File(input);
		List<LogRecord> logging = new ArrayList<LogRecord>();
		try {
			DataInputStream instream = new DataInputStream(new FileInputStream(infile));
			LogRecord log;
			while((log = deserialize(instream)) != null){
				logging.add(log);
			}
			instream.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RecoveryManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return logging;		
	}

	private static void serialize(LogRecord record, DataOutputStream stream) throws IOException {
		if (record instanceof BeginLogRecord) {
			BeginLogRecord beginRecord = (BeginLogRecord) record;
			stream.writeByte(BeginLogRecord.BEGIN);
			stream.writeInt(beginRecord.getTransactionId());
			return;
		}
		if (record instanceof CommitLogRecord) {
			CommitLogRecord commitRecord = (CommitLogRecord) record;
			stream.writeByte(CommitLogRecord.COMMIT);
			stream.writeInt(commitRecord.getTransactionId());
			return;
		}
		if (record instanceof AbortLogRecord) {
			AbortLogRecord abortRecord = (AbortLogRecord) record;
			stream.writeByte(AbortLogRecord.ABORT);
			stream.writeInt(abortRecord.getTransactionId());
			return;
		}
		if (record instanceof UpdateLogRecord) {
			UpdateLogRecord updateRecord = (UpdateLogRecord) record;
			stream.writeByte(UpdateLogRecord.UPDATE);
			stream.writeInt(updateRecord.getTransactionId());
			stream.writeInt(updateRecord.getPageId().getTableId());
			stream.writeInt(updateRecord.getPageId().getPageId());
			stream.writeShort(updateRecord.getLength());
			stream.writeShort(updateRecord.getOffset());
			stream.write(updateRecord.getBeforeImage());
			stream.write(updateRecord.getAfterImage());
		}
	}
	
	private static LogRecord deserialize(DataInputStream instream) throws IOException,RecoveryManagerException{
		byte type;
		try{
			type = instream.readByte();
		}catch (EOFException e) {
			DBLogger.info("final del archivo de log");
			return null;
		}
		if (type == BeginLogRecord.BEGIN) {
			BeginLogRecord beginRecord = new BeginLogRecord(instream.readInt());
			return beginRecord;
		}
		else if (type == CommitLogRecord.COMMIT) {
			CommitLogRecord commitRecord = new CommitLogRecord(instream.readInt());
			return commitRecord;
		}
		else if (type == AbortLogRecord.ABORT) {
			AbortLogRecord abortRecord = new AbortLogRecord(instream.readInt());
			return abortRecord;
		}
		else if (type == UpdateLogRecord.UPDATE) {
			
			int idtrans = instream.readInt();
			PageIdentifier pi = new PageIdentifier(instream.readInt(),instream.readInt());
			short lenght = instream.readShort();
			short offset = instream.readShort();
			byte[] beforeImage = new byte[lenght];
			byte[] afterImage = new byte[lenght];
			instream.read(beforeImage);
			instream.read(afterImage);
			UpdateLogRecord updateRecord = new UpdateLogRecord(idtrans,pi,lenght,offset,beforeImage,afterImage);
			return updateRecord;
		}else{
			DBLogger.info("archivos de log corrupto: el type "+(int)type+" no es valido");
			throw new RecoveryManagerException("type invalido: "+(int)type);
		}	
	}
}
