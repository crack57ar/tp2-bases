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
 *	M�dulo de "Recuperaci�n ante fallas" 
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
	
	//[start] M�todos p�blicos
	
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
	 * Guarda en disco los registros que hay en memoria y vac�a la lista de registros en memoria
	 */
	public void flushLog() throws RecoveryManagerException
	{
		//TODO: Completar 
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
	}
	//[end]
	
	//[end]

	//[start] M�todos privados
	
	//[start] 	analyzeLog
	/**
	 *	Recorre el log del disco, armando las 3 listas m�s el diccionario con las acciones de cada transacci�n 
	 */
	private void analyzeLog(Map<Long, List<LogRecord>> transactionRecords, List<Long> unfinishedTransactionIds, List<Long> abortedTransactionIds, List<Long> committedTransactionIds)
	{
		//TODO: Es opcional usar este m�todo pero est� para orientar
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
	}
	//[end]

	//[start] 	updatePage
	/**
	 * Actualiza la p�gina indicada en disco
	 */
	private void updatePage(long transactionId, PageIdentifier pageId, int lenght, int offset, byte[] image)
	{
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
