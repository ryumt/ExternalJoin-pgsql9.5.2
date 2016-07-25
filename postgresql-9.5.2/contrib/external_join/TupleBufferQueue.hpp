#ifndef TUPLEBUFFERQUEUE_HEAD_
#define TUPLEBUFFERQUEUE_HEAD_

// #include <atomic>

class TupleBufferQueue {
private: 
	static constexpr int QUEUE_LENGTH = 10;
	TupleBuffer *queue[QUEUE_LENGTH];
	std::atomic_int head;
	std::atomic_int tail;
	std::atomic_int length;
	
public: 
	TupleBufferQueue(void) { this->init(); }
	~TupleBufferQueue(void) { this->fini(); }
	
	static TupleBufferQueue *constructor(void) {
		TupleBufferQueue *tbq = static_cast<TupleBufferQueue *>(palloc(sizeof(*tbq)));
		tbq->init();
		return tbq;
	}
	static void destructor(TupleBufferQueue *tbq) {
		tbq->fini();
		pfree(tbq);
	}
	
	void init(void) {
		this->head.store(-1, std::memory_order_relaxed);
		this->tail.store(-1, std::memory_order_relaxed);
		this->length.store(0, std::memory_order_relaxed);
		std::atomic_thread_fence(std::memory_order_release);
		// this->head = this->length = -1;
		// this->length = 0;
	}
	void fini(void) {
		this->length.store(-1, std::memory_order_release);
	}
	
	int 
	push(TupleBuffer *tb) {
		int offset = ++(this->head);
		
		if (offset > QUEUE_LENGTH) {
			--(this->head);
			return -1;
		}
		queue[offset] = tb;
		++(this->length);
		return offset;		
	}
	
	TupleBuffer * 
	pop(void) {
		int offset;
		
		if (this->length.load(std::memory_order_relaxed) < 1)
			return NULL;
		offset = ++(this->tail);
		--(this->length);
		return queue[offset];			
	}
	
	int 
	getLength(void) const {
		return this->length.load(std::memory_order_relaxed);
	}
};
#endif //TUPLEBUFFERQUEUE_HEAD_

