package ubadb.services.recoveryManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ubadb.common.PageIdentifier;
import ubadb.components.DBComponentsEnum;
import ubadb.components.properties.DBProperties;
import ubadb.dbserver.DBServer;
import ubadb.logger.DBLogger;
import ubadb.services.DBService;
import ubadb.services.exceptions.DBServiceException;
import ubadb.services.recoveryManager.exceptions.RecoveryManagerException;
import ubadb.services.recoveryManager.logRecords.*;

/**
 *	M�dulo de "Recuperaci�n ante fallas" 
 */
public class RecoveryManager extends DBService
{
	//[start] Atributos
	private List<LogRecord> logRecordsInMemory;
	private int maxLogRecord;
	private String logFileName;
	private List<LogRecord> logRecords ;
	
	private Set<Integer> commitedTransaction;
	private Set<Integer> abortedTransaction;
	private Set<Integer> incompleteTransaction;
	//[end]
	
	//[start] Constructor
	public RecoveryManager()
	{
		logRecordsInMemory = new ArrayList<LogRecord>();
		maxLogRecord = ((DBProperties)DBServer.getComponent(DBComponentsEnum.PROPERTIES)).RecoveryManagerMaxLogRecordsInMemory();
		logFileName = ((DBProperties)DBServer.getComponent(DBComponentsEnum.PROPERTIES)).RecoveryManagerLogFileName();
		logRecords = ParseLog.getFromFile(logFileName);
		commitedTransaction = new HashSet<Integer>();
		abortedTransaction = new HashSet<Integer>();
		incompleteTransaction = new HashSet<Integer>();		
	}
	//[end]
	
	//[start] M�todos p�blicos
	
	//[start] 	addLogRecord
	/**
	 *	Agrega un registro al Log.
	 *	Si supera la capacidad de registros de log en memoria, se hace un flush y se agrega el record a memoria.
	 *	Si no, guarda el registro en memoria 
	 */
	public void addLogRecord(LogRecord logRecord) throws RecoveryManagerException
	{
		if(logRecordsInMemory.size() >= maxLogRecord){
			flushLog();
		}
		logRecordsInMemory.add(logRecord);
	}
	//[end]

	//[start] 	flushLog
	/**
	 * Guarda en disco los registros que hay en memoria y vac�a la lista de registros en memoria
	 */
	public void flushLog() throws RecoveryManagerException
	{
		ParseLog.saveToFile(logRecordsInMemory, logFileName);
		logRecordsInMemory = new ArrayList<LogRecord>();
	}
	//[end]
	
	//[start] 	recoverFromCrash
	/**
	 *	Implementa el algoritmo de recuperaci�n UNDO/REDO sin checkpoints 
	 */
	public void recoverFromCrash() throws RecoveryManagerException
	{
		//TODO: Completar
		//Sigo los pasos del algoritmo de recuperaci�n UNDO/REDO sin checkpointing
		//analizo el log por partes y dejo la info en las estructuras
		try {
			//analizo el log para obtener las listas
			analyzeLog(logRecords);
			//hago el undo
			undoTransaction(logRecords);
			// hago el redo 
			redoTransaction(logRecords);
			//agrego un abort por cada transaccion incompleta 
			for (Integer integer : incompleteTransaction) {
				addLogRecord(new AbortLogRecord(integer));
			}
			flushLog();
		} catch (RecoveryManagerException e) {
			e.printStackTrace();
		}
		
	}
	//[end]
	
	//[end]

	//[start] M�todos privados
	
	//[start] 	analyzeLog
	/**
	 *	Recorre el log del disco, armando las 3 listas: commiteadas, abortadas y incompletas.
	 *
	 */

	private void analyzeLog(List<LogRecord> logRecords)
	{
		//busco todas la trans commiteadas y luego las barro haciendo todas sus acciones nuevamente.
		
		for (int i = logRecords.size()-1; i >= 0; i--) {
			LogRecord logRecord = logRecords.get(i);
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
				if(!abortedTransaction.contains(begin.getTransactionId()) && 
				   !commitedTransaction.contains(begin.getTransactionId())&&
				   !incompleteTransaction.contains(begin.getTransactionId()))
					
					incompleteTransaction.add(begin.getTransactionId());
			}
			
		}
	}
	//[end]
	
	//[start] 	redoTransaction
	/**
	 *	 Rehace una transacci�n (por ahora, el �nico record que se debe rehacer es el UpdateLogRecord)
	 *	 IMPORTANTE: no hace nada con el commit
	 */
	private void redoTransaction(List<LogRecord> logRecords)
	{
		//Rehace cada paso de la transacci�n (debe usar el updatePage con la imagen nueva)
		//TODO: Completar
		for (int i = 0; i < logRecords.size(); i++) {
			LogRecord redoRecord = logRecords.get(i); 
			if(redoRecord instanceof UpdateLogRecord ){
				UpdateLogRecord update = (UpdateLogRecord) redoRecord;
				if(commitedTransaction.contains(update.getTransactionId())){
					DBLogger.debug("--> redoing change :");
					updatePage(update.getTransactionId(), update.getPageId(), update.getLength(), update.getOffset(), update.getAfterImage());
				}
			}
		}
	}
	//[end]
	
	//[start] 	undoTransaction
	/**
	 * Deshace una transacci�n (por ahora, el �nico record que se debe deshacer es el UpdateLogRecord)
	 * IMPORTANTE: no hace nada con el abort
	 */
	private void undoTransaction(List<LogRecord> logRecords)
	{
		//TODO: Completar
		//Deshace cada paso de la transacci�n (debe usar el updatePage con la imagen anterior)
		//NO pone abort en el log
		for (int i = logRecords.size()-1; i >= 0; i--) {
			LogRecord logRecord = logRecords.get(i);
			if(logRecord instanceof UpdateLogRecord ){
				UpdateLogRecord update = (UpdateLogRecord)logRecord;
				if(incompleteTransaction.contains(update.getTransactionId())){
					DBLogger.debug("<-- undoing change :");
					updatePage(update.getTransactionId(), update.getPageId(), update.getLength(), update.getOffset(), update.getBeforeImage());
				}
			}
		}
	}
	//[end]

	//[start] 	updatePage
	/**
	 * Actualiza la p�gina indicada en disco
	 */
	private void updatePage(long transactionId, PageIdentifier pageId, short lenght, short offset, byte[] image)
	{
		DBLogger.debug("cambiando item de la pag. <"+pageId.getPageId()+","+pageId.getTableId()+"> por la transaccion "+transactionId);
		//Esta llamada quedar� para completar cuando se vayan agregando diferentes m�dulos al sistema
	}
	//[end]
	
	//[end]
	
	//[start] M�todos abstractos
	
	//[start] 	initializeService
	@Override
	public void initializeService() throws DBServiceException
	{
		//Nada para hacer aqu�...
	}
	//[end]
	
	//[start]	finalizeService
	@Override
	public void finalizeService() throws DBServiceException
	{
		//Nada para hacer aqu�...		
	}
	//[end]

	//[start]	startService
	@Override
	public void startService() throws DBServiceException
	{
		//Nada para hacer aqu�...		
	}
	//[end]

	//[start]	stopService
	@Override
	public void stopService() throws DBServiceException
	{
		//Nada para hacer aqu�...		
	}
	//[end]

	//[end]
}
