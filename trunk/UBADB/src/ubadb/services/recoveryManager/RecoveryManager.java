package ubadb.services.recoveryManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import ubadb.common.PageIdentifier;
import ubadb.components.DBComponentsEnum;
import ubadb.components.properties.DBProperties;
import ubadb.dbserver.DBFactory;
import ubadb.logger.DBLogger;
import ubadb.services.DBService;
import ubadb.services.exceptions.DBServiceException;
import ubadb.services.recoveryManager.exceptions.RecoveryManagerException;
import ubadb.services.recoveryManager.logRecords.*;

/**
 *	Módulo de "Recuperación ante fallas" 
 */
public class RecoveryManager extends DBService
{
	//[start] Atributos
	private List<LogRecord> logRecordsInMemory;
	private int maxLogRecord = ((DBProperties)DBFactory.getComponents().get(DBComponentsEnum.PROPERTIES)).RecoveryManagerMaxLogRecordsInMemory();
	private String logFileName = ((DBProperties)DBFactory.getComponents().get(DBComponentsEnum.PROPERTIES)).RecoveryManagerLogFileName();
	//[end]
	
	//[start] Constructor
	public RecoveryManager()
	{
		//tomo el log de disco y lo traigo a memoria.
		logRecordsInMemory = ParseLog.getFromFile(logFileName);	
	}
	//[end]
	
	//[start] Métodos públicos
	
	//[start] 	addLogRecord
	/**
	 *	Agrega un registro al Log.
	 *	Si supera la capacidad de registros de log en memoria, se hace un flush y se agrega el record a memoria.
	 *	Si no, guarda el registro en memoria 
	 */
	public void addLogRecord(LogRecord logRecord) throws RecoveryManagerException
	{
		if(logRecordsInMemory.size() > maxLogRecord){
			flushLog();
		}
		logRecordsInMemory.add(logRecord);
	}
	//[end]

	//[start] 	flushLog
	/**
	 * Guarda en disco los registros que hay en memoria y vacía la lista de registros en memoria
	 */
	public void flushLog() throws RecoveryManagerException
	{
		ParseLog.saveToFile(logRecordsInMemory, logFileName);
		logRecordsInMemory = new ArrayList<LogRecord>();
	}
	//[end]
	
	//[start] 	recoverFromCrash
	/**
	 *	Implementa el algoritmo de recuperación UNDO/REDO sin checkpoints 
	 */
	public void recoverFromCrash() throws RecoveryManagerException
	{
		//TODO: Completar
		//Sigo los pasos del algoritmo de recuperación UNDO/REDO sin checkpointing
		undoTransaction(logRecordsInMemory);
		redoTransaction(logRecordsInMemory);
	}
	//[end]
	
	//[end]

	//[start] Métodos privados
	
	//[start] 	analyzeLog
	/**
	 *	Recorre el log del disco, armando las 3 listas más el diccionario con las acciones de cada transacción
	 *
	 *  $$ Metodo al dope $$
	 */
	@Deprecated
	private void analyzeLog(Map<Integer, List<LogRecord>> transactionRecords, List<Integer> unfinishedTransactionIds, List<Integer> abortedTransactionIds, List<Long> committedTransactionIds)
	{
		for (LogRecord record : logRecordsInMemory) {
			if(record instanceof CommitLogRecord){
				transactionRecords.put(((CommitLogRecord)record).getTransactionId(), new ArrayList<LogRecord>());
			}else if(record instanceof AbortLogRecord){
				abortedTransactionIds.add(((AbortLogRecord)record).getTransactionId());
			}else if(record instanceof UpdateLogRecord){
				if(transactionRecords.containsKey(((UpdateLogRecord)record).getTransactionId())){
					transactionRecords.get(((UpdateLogRecord)record).getTransactionId()).add(record);
				}else{
					if(unfinishedTransactionIds.contains(((UpdateLogRecord)record).getTransactionId()))
						unfinishedTransactionIds.add(((UpdateLogRecord)record).getTransactionId());
				}
			}else if(record instanceof BeginLogRecord){
				//si no es commiteada ni abortada, entonces es incompleta con solo el begin
				if(!transactionRecords.containsKey(((BeginLogRecord)record).getTransactionId()) &&
				   !abortedTransactionIds.contains(((BeginLogRecord)record).getTransactionId())	){
					
					unfinishedTransactionIds.add(((BeginLogRecord)record).getTransactionId());
				}	
			}
			
		}
	}
	//[end]
	
	//[start] 	redoTransaction
	/**
	 *	 Rehace una transacción (por ahora, el único record que se debe rehacer es el UpdateLogRecord)
	 *	 IMPORTANTE: no hace nada con el commit
	 */
	private void redoTransaction(List<LogRecord> logRecords)
	{
		//Rehace cada paso de la transacción (debe usar el updatePage con la imagen nueva)
		//TODO: Completar
		
		//busco todas la trans commiteadas y luego las barro haciendo todas sus acciones nuevamente.
		List<Integer> commitedTransaction = new ArrayList<Integer>();
		List<Integer> abortedTransaction = new ArrayList<Integer>();
		List<Integer> incompleteTransaction = new ArrayList<Integer>();
		
		for (LogRecord logRecord : logRecords) {
			if(logRecord instanceof CommitLogRecord ){
				CommitLogRecord commit = (CommitLogRecord) logRecord;
				commitedTransaction.add(commit.getTransactionId());
			}else if(logRecord instanceof AbortLogRecord){
				AbortLogRecord abort = (AbortLogRecord) logRecord;
				abortedTransaction.add(abort.getTransactionId());
			}else if(logRecord instanceof UpdateLogRecord){
				UpdateLogRecord update = (UpdateLogRecord) logRecord;
				if(!abortedTransaction.contains(update.getTransactionId()) && !commitedTransaction.contains(update.getTransactionId()))
					incompleteTransaction.add(update.getTransactionId());
			}else if(logRecord instanceof BeginLogRecord){
				BeginLogRecord begin = (BeginLogRecord) logRecord;
				if(!abortedTransaction.contains(begin.getTransactionId()) && !commitedTransaction.contains(begin.getTransactionId()))
					incompleteTransaction.add(begin.getTransactionId());
			}
			
		}
		for (int i = 0; i < logRecords.size(); i++) {
			LogRecord redoRecord = logRecords.get(i); 
			if(redoRecord instanceof UpdateLogRecord ){
				UpdateLogRecord update = (UpdateLogRecord) redoRecord;
				if(commitedTransaction.contains(update.getTransactionId())){
					updatePage(update.getTransactionId(), update.getPageId(), update.getLength(), update.getOffset(), update.getAfterImage());
				}
			}
		}
		
		try {
			for (Integer integer : incompleteTransaction) {
				//agrego un abort por cada transaccion incompleta
				addLogRecord(new AbortLogRecord(integer));
			}
			flushLog();
		} catch (RecoveryManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	//[end]
	
	//[start] 	undoTransaction
	/**
	 * Deshace una transacción (por ahora, el único record que se debe deshacer es el UpdateLogRecord)
	 * IMPORTANTE: no hace nada con el abort
	 */
	private void undoTransaction(List<LogRecord> logRecords)
	{
		//TODO: Completar
		//Deshace cada paso de la transacción (debe usar el updatePage con la imagen anterior)
		//NO pone abort en el log
		List<Integer> finishedTransactions = new ArrayList<Integer>();
		
		for (LogRecord logRecord : logRecords) {
			if(logRecord instanceof UpdateLogRecord ){
				UpdateLogRecord update = (UpdateLogRecord)logRecord;
				if(!finishedTransactions.contains(update.getTransactionId())){
					updatePage(update.getTransactionId(), update.getPageId(), update.getLength(), update.getOffset(), update.getBeforeImage());
				}
			}else if(logRecord instanceof CommitLogRecord ){
				CommitLogRecord commit = (CommitLogRecord) logRecord;
				if(!finishedTransactions.contains(commit.getTransactionId())){
					finishedTransactions.add(commit.getTransactionId());
				}
			}else if(logRecord instanceof AbortLogRecord ){
				AbortLogRecord abort = (AbortLogRecord) logRecord;
				if(!finishedTransactions.contains(abort.getTransactionId())){
					finishedTransactions.add(abort.getTransactionId());
				}
			}else{
				DBLogger.debug("El Log encontrado no es reconocido por el sistema: "+logRecord.getClass());
			}
		}
	}
	//[end]

	//[start] 	updatePage
	/**
	 * Actualiza la página indicada en disco
	 */
	private void updatePage(long transactionId, PageIdentifier pageId, int lenght, int offset, byte[] image)
	{
		//Esta llamada quedará para completar cuando se vayan agregando diferentes módulos al sistema
	}
	//[end]
	
	//[end]
	
	//[start] Métodos abstractos
	
	//[start] 	initializeService
	@Override
	public void initializeService() throws DBServiceException
	{
		//Nada para hacer aquí...
	}
	//[end]
	
	//[start]	finalizeService
	@Override
	public void finalizeService() throws DBServiceException
	{
		//Nada para hacer aquí...		
	}
	//[end]

	//[start]	startService
	@Override
	public void startService() throws DBServiceException
	{
		//Nada para hacer aquí...		
	}
	//[end]

	//[start]	stopService
	@Override
	public void stopService() throws DBServiceException
	{
		//Nada para hacer aquí...		
	}
	//[end]

	//[end]
}
