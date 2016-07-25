#ifndef TUPLEBUFFER_HEAD_
#define TUPLEBUFFER_HEAD_

class TupleBuffer {
private:
	static constexpr std::size_t INITIAL_BUFSIZE = 1024UL * 1024UL * 32;
	
	void *buffer;
	std::size_t content_size;
	std::size_t buffer_size;
	
public:
	TupleBuffer(void) { this->init(); }
	~TupleBuffer(void) { this->fini(); }
	
	static TupleBuffer *constructor(void) {
		TupleBuffer *tb = static_cast<TupleBuffer *>(palloc(sizeof(*tb)));
		tb->init();
		return tb;
	}
	static void destructor(TupleBuffer *tb) {
		tb->fini();
		pfree(tb);
	}
	
	void init(void) {
		this->buffer = palloc(TupleBuffer::INITIAL_BUFSIZE);
		this->buffer_size = TupleBuffer::INITIAL_BUFSIZE;
		this->content_size = 0;
	}
	void fini(void) {
		pfree(this->buffer);
	}
		
	bool 
	checkOverflow(std::size_t data_size) const {
		return (this->content_size + data_size >= this->buffer_size);
	}
	
	void 
	extendBuffer(void) {
		this->buffer_size *= 2;
		/* may cause memory shortage */
		this->buffer = repalloc_huge(this->buffer, this->buffer_size);
	}
	
	void 
	putTuple(TupleTableSlot *tts) {
		std::size_t tuple_size = TupleBuffer::getTupleSize(tts);
		
		while (this->checkOverflow(tuple_size))
			this->extendBuffer();
		
		std::memcpy(this->getWritePointer(), TupleBuffer::getTupleDataPointer(tts), tuple_size);
		this->content_size += tuple_size;
	}
	
	void * 
	getWritePointer(void) const {
		return static_cast<void *>(static_cast<char *>(this->buffer) + this->content_size);
	}

	void * 
	getBufferPointer(void) const {
		return this->buffer;
	}
	
	std::size_t 
	getContentSize(void) const {
		return this->content_size;
	}
	
	static 
	std::size_t 
	getTupleSize(TupleTableSlot *tts) {
		return (tts->tts_tuple->t_len - tts->tts_tuple->t_data->t_hoff);
	}
	
	static 
	void * 
	getTupleDataPointer(TupleTableSlot *tts) {
		HeapTupleHeader ht = tts->tts_tuple->t_data;
		return static_cast<void *>(reinterpret_cast<char *>(ht) + ht->t_hoff);
	}
};

#endif //TUPLEBUFFER_HEAD_
