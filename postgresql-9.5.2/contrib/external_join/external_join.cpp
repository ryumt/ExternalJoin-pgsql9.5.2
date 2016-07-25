/*-------------------------------------------------------------------------
 *
 * external_join.c
 *
 *
 * Copyright (c) 2016, Ryuya Mitsuhashi
 *
 * IDENTIFICATION
 *	  contrib/external_join/external_join.cpp
 *
 *-------------------------------------------------------------------------
 */
#define BEGIN_C_SPACE extern "C" {
#define END_C_SPACE }

/* We cannot use new/delete because query cancelation may cause memory leak. */
/* We cannot use std::thread because namespace problem with C linkage [template<> should not be used in C linkage] */
// #include <iostream>
// #include <thread>
#include <atomic>
#include <cstdio>
#include <cstring>

BEGIN_C_SPACE 
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <strings.h>
#include <pthread.h>

#include <errno.h>

#include "postgres.h"

#include <limits.h>
#include "executor/executor.h"
#include "utils/guc.h"

#include "access/htup_details.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "nodes/print.h"


#include "TupleBuffer.hpp"
#include "TupleBufferQueue.hpp"
#include "ResultBuffer.hpp"
#include "socket_lapper.hpp"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

/* Saved hook values in case of unload */
static ExecProcNode_hook_type prev_ExecProcNode = NULL;

/* GUC variables */
/* Flag to use external join module */
static bool EnableExternalJoin = false;
/* Address for External Process */
static char *ExternalAddress = const_cast<char *>("127.0.0.1");
static int ExternalPort = 59999;

/* Initialized pthread_t */
static pthread_t InitialThread;
/* Holds currently running pthread_t, this is used when a query is cancelled */
static pthread_t PreviousThread;

/* State for external join */
/* ExternalExecProcNode() uses PlanState->initPlan field to hold execution state. This is scamped design. */
/* If initPlan is used by original query process, ExternalExecProcNode() cannot work fine. */
static constexpr NodeTag T_ExternalJoin = static_cast<NodeTag>(65535);
enum State { INIT = 0, EXEC, FINI };
struct ExternalJoinState {
	NodeTag type;
	State state;
	
	/* socket to communicate with external process */
	int sock;
	/* tuple sendeng or result receiving thread */
	pthread_t thread;
	
	/* send buffer queue */
	TupleBufferQueue tbq;
	
	/* result buffer: double buffered */
	DoubleResultBuffer drb;
	/* result buffer which result processing thread currently handles */
	ResultBuffer *prb;
	/* offset(cursor) to scan result buffer */
	std::size_t poffset; 
	/* base offset when an attribute sticks out of buffer */
	std::size_t pbase;
		
	/* size of content in result buffer */
	long psize;
};

static ExternalJoinState *makeExternalJoinState(void);
static void FreeExternalJoinState(ExternalJoinState *ejs);
static ExternalJoinState *GetExternalJoinState(List *initPlan);
static ExternalJoinState *SetExternalJoinState(List **initPlan, ExternalJoinState *ejs);

/* main function to be called */
static TupleTableSlot *ExternalExecProcNode(PlanState *ps);

/* external join executor */
static ExternalJoinState *InitExternalJoin(PlanState *ps);
static void EndExternalJoin(PlanState *ps);
static TupleTableSlot *ExecExternalJoin(PlanState *ps);

/* tuple scanner */
static void ScanTuple(PlanState *node, ExternalJoinState *ejs);
/* tuple sender */
static void *SendTupleToExternal(void *arg);
/* result receiver */
static void *ReceiveResultFromExternal(void *arg);

static uint64_t bytesExtract(uint64_t x, int n);
/* thread cancelling function */
static void CancelPreviousSessionThread(void);
static void SetCurrentSessionThread(pthread_t thread);


/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables. */
	DefineCustomBoolVariable("external_join.enable",
				 "Selects whether external join is enabled.",
                                 NULL,
                                 &EnableExternalJoin,
                                 false,
                                 PGC_USERSET,
                                 0,
                                 NULL,
				 NULL,
                                 NULL);
	
	DefineCustomIntVariable("external_join.port",
				"Selects what port external join connects to.",
				NULL,
				&ExternalPort, 
				59999, 
				0, 
				65535, 
				PGC_USERSET,
				0,
				NULL,
				NULL,
				NULL);
	
	DefineCustomStringVariable("external_join.addr",
				   "Selects where external join connects.",
				   NULL, 
				   &ExternalAddress, 
				   "127.0.0.1", 
				   PGC_USERSET,
				   0,
				   NULL,
				   NULL,
				   NULL);
	
        EmitWarningsOnPlaceholders("external_join");
	
	elog(DEBUG1, "----- external join module loaded -----");
	/* Install hooks. */
	prev_ExecProcNode = ExecProcNode_hook;
	ExecProcNode_hook = ExternalExecProcNode;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	elog(DEBUG1, "-----external join module unloaded-----"); 
	/* Uninstall hooks. */
	ExecProcNode_hook = prev_ExecProcNode;
}

static inline 
ExternalJoinState *
makeExternalJoinState(void)
{
	ExternalJoinState *ejs;
	
	ejs = static_cast<ExternalJoinState *>(palloc(sizeof(*ejs)));
	ejs->type = T_ExternalJoin;
	ejs->state = State::INIT;
	
	ejs->tbq.init();
	
	ejs->drb.init();
	ejs->prb = ejs->drb.getCurrentResultBuffer();
	ejs->poffset = ResultBuffer::BUFSIZE;
	ejs->pbase = 0;
	
	ejs->psize = 0;
	
	return ejs;
}

static inline 
void 
FreeExternalJoinState(ExternalJoinState *ejs)
{
	ejs->tbq.fini();
	ejs->drb.fini();
	pfree(ejs);
}

static inline 
ExternalJoinState *
GetExternalJoinState(List *initPlan)
{
	return reinterpret_cast<ExternalJoinState *>(initPlan);
}

static inline 
ExternalJoinState *
SetExternalJoinState(List **initPlan, ExternalJoinState *ejs)
{
	*reinterpret_cast<ExternalJoinState **>(initPlan) = ejs;
	return ejs;
}


/* main */
TupleTableSlot *
ExternalExecProcNode(PlanState *ps)
{
	TupleTableSlot *tts = NULL;
	ExternalJoinState *ejs = GetExternalJoinState(ps->initPlan);
	
	elog(DEBUG5, "*****ExternalExecProcNode()*****");
	if (EnableExternalJoin == false)
                return ExecProcNode(ps);
	
	if (!(ejs == NULL || (ejs->state >= State::INIT && ejs->state <= State::FINI))) {
		ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
				errmsg("PlanState->initPlan is set, cannot apply external join to this query.")));
	}
	
	for (;;) {
		if (ejs == NULL) {
			elog(DEBUG5, "BEGIN: Init");
			ejs = InitExternalJoin(ps);
			
			/* create result receiving thread */
			CancelPreviousSessionThread();
			if (::pthread_create(&ejs->thread, NULL, ReceiveResultFromExternal, static_cast<void *>(ejs)) < 0) {
				ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
						errmsg("cannot create thread in ::pthread_create()\n")));
			}
			SetCurrentSessionThread(ejs->thread);
			
			/* wait for first filling result buffer */
			ejs->poffset = 0;
			while ((ejs->psize = ejs->prb->getContentSize()) == 0)
				::usleep(1);
			
			ejs->state = State::EXEC;
			elog(DEBUG5, "END: Init");
		}
		else if (ejs->state == State::EXEC) {
			elog(DEBUG5, "BEGIN: Exec");
			
			tts = ExecExternalJoin(ps);
			if (tts == NULL) {
				ejs->state = State::FINI;
				continue;
			}
			
			elog(DEBUG5, "END: Exec");
			break;
		}
		else if (ejs->state == State::FINI) {
			elog(DEBUG5, "END: Begin");
			
			/* join result receiving thread */
			::pthread_join(ejs->thread, NULL);
			CancelPreviousSessionThread();
			EndExternalJoin(ps);
			
			elog(DEBUG5, "END: End");
			break;
		}
		else {
			ereport(FATAL, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), 
					errmsg("unknown state in ExternalExecProcNode()\n")));
			break;
		}
	};
	
	return tts;
}

static inline 
ExternalJoinState *
InitExternalJoin(PlanState *ps)
{
	ExternalJoinState *ejs = SetExternalJoinState(&ps->initPlan, makeExternalJoinState());
	
	/* connect to external process */
	ejs->sock = connectSock(ExternalAddress, ExternalPort);
	if (ejs->sock < 0) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), 
				errmsg("failed to connect %s:%d\n", ExternalAddress, ExternalPort)));
	}
	
	CancelPreviousSessionThread();
	/* create tuple sending thread */
	if (::pthread_create(&ejs->thread, NULL, SendTupleToExternal, static_cast<void *>(ejs)) < 0) {
		ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
				errmsg("cannot create thread in ::pthread_create()\n")));
	}
	SetCurrentSessionThread(ejs->thread);
	
	/* "tuple scan" and "tuple send" are executed concurrently */
	ScanTuple(ps, ejs);
	while (ejs->tbq.getLength() > 0)
		::usleep(1);
	ejs->tbq.fini();
	
       	ExecAssignExprContext(ps->state, ps);
	/* init result tuple */
	ExecInitResultTupleSlot(ps->state, ps);
	ExecAssignResultTypeFromTL(ps);
	ExecAssignProjectionInfo(ps, NULL);
	ps->ps_TupFromTlist = false;
	
	::pthread_join(ejs->thread, NULL);
	CancelPreviousSessionThread();
	return ejs;
}

static inline 
void 
EndExternalJoin(PlanState *ps)
{
	ExternalJoinState *ejs = GetExternalJoinState(ps->initPlan);
	
	ExecFreeExprContext(ps);
	ExecClearTuple(ps->ps_ResultTupleSlot);
	::close(ejs->sock);
	FreeExternalJoinState(ejs);
}


static inline
std::size_t 
GetAlignedOffset(std::size_t prev, std::size_t size)
{
	return (prev % size) ? (prev / size + 1) * size : prev;
}

static inline 
TupleTableSlot *
ExecExternalJoin(PlanState *ps)
{
	ExternalJoinState *ejs = GetExternalJoinState(ps->initPlan);
	TupleTableSlot *tts = ps->ps_ProjInfo->pi_slot;
	TupleDesc td = tts->tts_tupleDescriptor;
	/* if data sticks out of buffer, use this buffer to merge splitted data */
	uint64_t ovf = 0;
	
	/* wait until result buffer will be filled */
	if (ejs->poffset == ResultBuffer::BUFSIZE) {
		elog(DEBUG2, ":: ResultBuffer FULL switch");
		ejs->prb->setContentSize(0);
		ejs->drb.switchResultBuffer();
		
		ejs->poffset = 0;
		ejs->pbase = 0;
		ejs->prb = ejs->drb.getCurrentResultBuffer();
		while ((ejs->psize = ejs->prb->getContentSize()) == 0)
			::usleep(1);
		/* EOF */
		if (ejs->psize < 0)
			return NULL;
	}
	else if (ejs->poffset >= static_cast<std::size_t>(ejs->psize))
		return NULL;
	
	/* check cancel request */
	CHECK_FOR_INTERRUPTS();
	
	/* fill result tuple */
	ExecClearTuple(tts);
	for (int col = 0; col < td->natts; col++) {
		void *ptr;
		
		switch (td->attrs[col]->atttypid) {
		case INT8OID:
		case FLOAT8OID:
			ejs->poffset = GetAlignedOffset(ejs->poffset, sizeof(double));
			ptr = (*ejs->prb)[ejs->poffset + ejs->pbase];
			ejs->poffset += sizeof(double);
			break;
		case INT4OID:
                case FLOAT4OID:
		case OIDOID:
			ejs->poffset = GetAlignedOffset(ejs->poffset, sizeof(float));
			ptr = (*ejs->prb)[ejs->poffset + ejs->pbase];
			ejs->poffset += sizeof(float);
			break;
                case INT2OID:
			ejs->poffset = GetAlignedOffset(ejs->poffset, sizeof(short));
			ptr = (*ejs->prb)[ejs->poffset + ejs->pbase];
			ejs->poffset += sizeof(short);
			break;
		case BOOLOID: 
			ejs->poffset = GetAlignedOffset(ejs->poffset, sizeof(bool));
			ptr = (*ejs->prb)[ejs->poffset + ejs->pbase];
			ejs->poffset += sizeof(bool);
			break;
		default: 
			ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), 
					errmsg("unsupported result type %d.\n", td->attrs[col]->atttypid)));
		};
		
		
		/* an attribute sticks out of buffer */
		if (ejs->poffset + td->attrs[col]->attlen > ResultBuffer::BUFSIZE) {
			elog(DEBUG2, ":: ResultBuffer HUNGRY switch");
			int held_size = ResultBuffer::BUFSIZE - ejs->poffset;
			int remain_size = td->attrs[col]->attlen - held_size;
			uint64_t held, remain;
			
			/* get the first part of this attribute */
			held = bytesExtract(*(static_cast<uint64_t *>(ptr)), held_size - 1);
			
			/* switch buffer to get the remaining part of the attribute */
			ejs->prb->setContentSize(0);
			ejs->drb.switchResultBuffer();
			ejs->prb = ejs->drb.getCurrentResultBuffer();
			ejs->poffset = 0;
			ejs->pbase = remain_size;
			while ((ejs->psize = ejs->prb->getContentSize()) == 0)
				usleep(1);
			if (ejs->psize < 0) {
				perror("sock 1");
				ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), 
						errmsg("unexpected connection shutdown\n")));
			}
			
			/* get the last part of the attribute */
			remain = bytesExtract(*static_cast<uint64_t *>((*ejs->prb)[0]), remain_size - 1);
			remain <<= held_size * 8;
			
			/* merge first and last part of the attribute */
			ovf = held | remain;
			ptr = static_cast<void *>(&ovf);
		}
		
		/* put column data to result tuple */
		switch (td->attrs[col]->atttypid) {
		case BOOLOID: 
			tts->tts_values[col] = BoolGetDatum(*reinterpret_cast<bool *>(ptr));
			break;
                case INT8OID:
			tts->tts_values[col] = Int64GetDatum(*reinterpret_cast<int64_t *>(ptr));
			break;
                case INT2OID:
			tts->tts_values[col] = Int16GetDatum(*reinterpret_cast<int16_t *>(ptr));
			break;
		case INT4OID:
			tts->tts_values[col] = Int32GetDatum(*reinterpret_cast<int32_t *>(ptr));
			break;
                case FLOAT4OID:
			tts->tts_values[col] = Float4GetDatum(*reinterpret_cast<float *>(ptr));
			break;
		case FLOAT8OID:
			tts->tts_values[col] = Float8GetDatum(*reinterpret_cast<double *>(ptr));
			break;
		case OIDOID:
			tts->tts_values[col] = ObjectIdGetDatum(*reinterpret_cast<uint32_t *>(ptr));
			break;
		};
	}
	/* set null flags to false */
	::bzero(static_cast<void *>(tts->tts_isnull), sizeof(bool) * td->natts);
	
	return ExecStoreVirtualTuple(tts);
}

static 
void *
ReceiveResultFromExternal(void *arg)
{
	ExternalJoinState *ejs = static_cast<ExternalJoinState *>(arg);
	int sock = ejs->sock;
	DoubleResultBuffer *drb = &ejs->drb;
	ResultBuffer *rb;
	long csize;
	
	rb = drb->getCurrentResultBuffer();
	for (;;) {
		ResultBuffer *next_rb;
		
		/* wait until result buffer will become empty */
		pthread_testcancel();
		while ((csize = rb->getContentSize()) != 0) {
			pthread_testcancel();
			usleep(1);
		}
		/* EOF */
		if (drb->isTerminated())
			break;
		
		/* receive data and fill result buffer */
		csize = receiveStrong(sock, (*rb)[0], ResultBuffer::BUFSIZE);
		// printf("thread::csize = %ld\n", csize);
		/* connection was closed */
		if (csize == 0) {
			rb->setContentSize(-1);
			break;
		}
		if (csize < 0) {
			perror("sock error 2");
			ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), 
					errmsg("unexpected connection shutdown\n")));
		}
		
		rb->setContentSize(csize);
		next_rb = drb->getNextResultBuffer();
		if (next_rb == rb) 
			next_rb = drb->getCurrentResultBuffer();
		rb = next_rb;
	}
	
	return NULL;
}

static 
void * 
SendTupleToExternal(void *arg)
{
	ExternalJoinState *ejs = static_cast<ExternalJoinState *>(arg);
	int sock = ejs->sock;
	
	/* if TupleBufferQueue is finalized, TupleBufferQueue->getLength() returns -1 */
	while (ejs->tbq.getLength() >= 0) {
		TupleBuffer *tb = ejs->tbq.pop();
		std::size_t size;
		
		/* wait for scan completion */
		pthread_testcancel();
		if (tb == NULL) {
			::usleep(1);
			continue;
		}
		size = tb->getContentSize();
		/* send tuple buffer size to external */
		sendStrong(sock, &size, sizeof(size));
		/* send tuples to external */
		sendStrong(sock, tb->getBufferPointer(), size);
		TupleBuffer::destructor(tb);
	}
	return NULL;
}

static inline 
void 
ScanTuple(PlanState *node, ExternalJoinState *ejs)
{
	if (node == NULL)
		return ;
	if (node->type >= T_ScanState && node->type <= T_CustomScanState) {
		TupleBuffer *tb = TupleBuffer::constructor();
		
		elog(DEBUG5, "----- ScanNode [%p] -----", node);
		elog_node_display(DEBUG5, "ScanNode->plan", node->plan, true);
		
		/* scan tuple */
		for (TupleTableSlot *tts = ExecProcNode(node); tts->tts_isempty == false; tts = ExecProcNode(node)) {
			/* copy tuple to buffer */
			tb->putTuple(tts);
			ResetExprContext(node->ps_ExprContext);
		}
		
		/* scan is complete for this ScanNode, put buffer into queue */
		ejs->tbq.push(tb);
	}
	/* look for other ScanNode */
	ScanTuple(outerPlanState(node), ejs);
	ScanTuple(innerPlanState(node), ejs);
}


static inline 
uint64_t 
bytesExtract(uint64_t x, int n)
{
	static constexpr uint64_t TABLE[] = {
		0x00000000000000FF, 0x000000000000FFFF, 0x0000000000FFFFFF, 0x00000000FFFFFFFF, 
		0x000000FFFFFFFFFF, 0x0000FFFFFFFFFFFF, 0x00FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF
	};
	return x & TABLE[n];
}

static inline 
void 
CancelPreviousSessionThread(void)
{
	/* cancel previously not cancelled thread */
	if (!pthread_equal(PreviousThread, InitialThread)) {
		pthread_cancel(PreviousThread);
		memcpy(static_cast<void *>(&PreviousThread), static_cast<void *>(&InitialThread), sizeof(InitialThread));
	}
}

static inline 
void 
SetCurrentSessionThread(pthread_t thread)
{
	/* set current thread for later cancel */
	memcpy(static_cast<void *>(&PreviousThread), static_cast<void *>(&thread), sizeof(InitialThread));
}
END_C_SPACE

