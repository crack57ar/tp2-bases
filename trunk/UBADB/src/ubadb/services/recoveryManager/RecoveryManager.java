package ubadb.services.recoveryManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ubadb.common.PageIdentifier;
import ubadb.services.DBService;
import ubadb.services.exceptions.DBServiceException;
import ubadb.services.recoveryManager.exceptions.RecoveryManagerException;
import ubadb.services.recoveryManager.logRecords.LogRecord;

/**
 *	Módulo de "Recuperación ante fallas" 
 */
public class RecoveryManager extends DBService
{
	//[start] Atributos
	private List<LogRecord> logRecordsInMemory;
	//[end]
	
	//[start] Constructor
	public RecoveryManager()
	{
		//TODO: Completar
		logRecordsInMemory = new ArrayList<LogRecord>();
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
		//TODO: Completar
	}
	//[end]

	//[start] 	flushLog
	/**
	 * Guarda en disco los registros que hay en memoria y vacía la lista de registros en memoria
	 */
	public void flushLog() throws RecoveryManagerException
	{
		//TODO: Completar 
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
	}
	//[end]
	
	//[end]

	//[start] Métodos privados
	
	//[start] 	analyzeLog
	/**
	 *	Recorre el log del disco, armando las 3 listas más el diccionario con las acciones de cada transacción 
	 */
	private void analyzeLog(Map<Long, List<LogRecord>> transactionRecords, List<Long> unfinishedTransactionIds, List<Long> abortedTransactionIds, List<Long> committedTransactionIds)
	{
		//TODO: Es opcional usar este método pero está para orientar
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
