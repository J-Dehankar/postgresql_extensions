#include "postgres.h"

#include "access/transam.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "storage/ipc.h"
#include "utils/builtins.h"

#define EXTENSION_NAME " testext()"

PG_MODULE_MAGIC;


/*
 * Global shared state
 */
typedef struct mySharedState
{
	LWLock *lock;

	int count_unknown;
	int count_select;
	int count_update;
	int count_insert;
	int count_delete;
	int count_utility;
	int count_nothing;

} mySharedState;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

/* Links to shared memory state */
static mySharedState *ptr = NULL;

/*---- Function declarations ----*/
void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(testext);

static void myfunc_shmem_startup(void);
static void myfunc_ExecutorEnd(QueryDesc *queryDesc);


/*
 * Module load callback
 */
void _PG_init(void) {
	
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the pg_stat_statements functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in myfunc_shmem_startup().
	 */
	RequestAddinShmemSpace(sizeof(mySharedState));
	RequestNamedLWLockTranche("testext", 1);

	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = myfunc_shmem_startup;
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = myfunc_ExecutorEnd;
}


/*
 * Module unload callback
 */
void _PG_fini(void) {
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorEnd_hook = prev_ExecutorEnd;
}


static void
myfunc_shmem_startup(void) {

	bool found;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ptr = ShmemInitStruct("testext", sizeof(mySharedState), &found);
	
	if (!found) {

		// initialize contents of shmem area;
		// acquire any requested LWLocks using:
		ptr->lock = &(GetNamedLWLockTranche("testext"))->lock;

		ptr->count_unknown = 0;
		ptr->count_select = 0;
		ptr->count_update = 0;
		ptr->count_insert = 0;
		ptr->count_delete = 0;
		ptr->count_utility = 0;
		ptr->count_nothing = 0;

	}
	LWLockRelease(AddinShmemInitLock);

}


// main function
Datum testext(PG_FUNCTION_ARGS)
{    
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	rsinfo->returnMode = SFRM_Materialize;

	MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	MemoryContext oldcontext = MemoryContextSwitchTo(per_query_ctx);	

	Tuplestorestate *tupstore = tuplestore_begin_heap(false, false, work_mem);
	rsinfo->setResult = tupstore;
	
	TupleDesc tupdesc = rsinfo->expectedDesc;
	
	Datum values[2];
	bool nulls[2] = {false};

	values[0] = CStringGetTextDatum("UNKNOWN");
	values[1] = Int64GetDatum(ptr->count_unknown);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	values[0] = CStringGetTextDatum("SELECT");
	values[1] = Int64GetDatum(ptr->count_select);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	values[0] = CStringGetTextDatum("UPDATE");
	values[1] = Int64GetDatum(ptr->count_update);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	values[0] = CStringGetTextDatum("INSERT");
	values[1] = Int64GetDatum(ptr->count_insert);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	values[0] = CStringGetTextDatum("DELETE");
	values[1] = Int64GetDatum(ptr->count_delete);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	values[0] = CStringGetTextDatum("UTILITY");
	values[1] = Int64GetDatum(ptr->count_utility);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	values[0] = CStringGetTextDatum("NOTHING");
	values[1] = Int64GetDatum(ptr->count_nothing);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);


	tuplestore_donestoring(tupstore);
	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_NULL();
}


static void
myfunc_ExecutorEnd(QueryDesc *queryDesc)
{    
	enum CmdType type;
	type = queryDesc->operation;


	if(type == CMD_SELECT && strstr(queryDesc->sourceText, EXTENSION_NAME)) {
		standard_ExecutorEnd(queryDesc);
		return;
	}

	// Acquire LW_EXCLUSIVE lock to write/update into shared memory variable
	LWLockAcquire(ptr->lock, LW_EXCLUSIVE);
	
	switch (type)
	{
	case CMD_UNKNOWN:
		ptr->count_unknown++;
		break;

	case CMD_SELECT:
		ptr->count_select++;
		break;

	case CMD_UPDATE:
		ptr->count_update++;
		break;

	case CMD_INSERT:
		ptr->count_insert++;
		break;

	case CMD_DELETE:
		ptr->count_delete++;
		break;

	case CMD_UTILITY:
		ptr->count_utility++;
		break;

	case CMD_NOTHING:
		ptr->count_nothing++;
		break;
	
	default:
		break;
	}
	
	// Release lock after use
	LWLockRelease(ptr->lock);

	// if there exists any previous executor hook then give back control to it
	if(prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}
