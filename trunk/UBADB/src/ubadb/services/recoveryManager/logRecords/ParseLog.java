package ubadb.services.recoveryManager.logRecords;

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

import ubadb.common.PageIdentifier;
import ubadb.logger.DBLogger;
import ubadb.services.recoveryManager.RecoveryManager;
import ubadb.services.recoveryManager.exceptions.RecoveryManagerException;

public class ParseLog {

	public static void main(String[] args)
	{
		String outputLog = "out/RecoveryManagerLog.dat";
		
		List<LogRecord> records = generateRecords();
		
		saveToFile(records, outputLog);
		
		records = getFromFile(outputLog);
		
	}

	private static List<LogRecord> generateRecords()
	{
		List<LogRecord> ret = new ArrayList<LogRecord>();
		
		//Acá va lo que uno quiere generar
		
		ret.add(new BeginLogRecord(1));
		ret.add(new BeginLogRecord(2));
		
		ret.add(new UpdateLogRecord(1,new PageIdentifier(10,20),(short)2,(short)0,new byte[]{0,0},new byte[]{1,1}));
		
		ret.add(new CommitLogRecord(1));
		ret.add(new AbortLogRecord(2));
		
		return ret;
	}

	
	/** 
	 * metodo para contar el largo del archivo en cantidad de Logs.
	 * **/
	public static int size(String input){
		File infile = new File(input);
		int LogSize = 0;
		try {
			DataInputStream instream = new DataInputStream(new FileInputStream(infile));
			while(deserialize(instream) != null){
				LogSize++;
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
		return LogSize;
	}
	
	public static void saveToFile(List<LogRecord> records, String outputLog)
	{
		//Escribo el arreglo de bytes de cada record en el archivo de salida
		//new File(outputLog).delete();
		try {
			DataOutputStream stream = new DataOutputStream(new FileOutputStream(outputLog,true));
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
	
	public static List<LogRecord> getFromFile(String input) {
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
