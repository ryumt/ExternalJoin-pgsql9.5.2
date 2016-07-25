#ifndef RESULTBUFFER_HEAD_
#define RESULTBUFFER_HEAD_

class ResultBuffer {
public: 
	static constexpr std::size_t BUFSIZE = 1024UL * 1024UL * 128;
	// static constexpr std::size_t BUFSIZE = 30UL;
private: 
	void *buffer;
	std::atomic_long content_size;
	
public: 
	ResultBuffer(void) { this->init(); }
	~ResultBuffer(void) { this->fini(); }
	
	static ResultBuffer *constructor(void) {
                ResultBuffer *rb = static_cast<ResultBuffer *>(palloc(sizeof(*rb)));
                rb->init();
                return rb;
        }
        static void destructor(ResultBuffer *rb) {
		rb->fini();
                pfree(rb);
        }
	
	void init(void) {
                this->buffer = palloc(ResultBuffer::BUFSIZE);
                this->content_size.store(0, std::memory_order_relaxed);
	}
        void fini(void) {
                pfree(this->buffer);
		this->content_size.store(0, std::memory_order_relaxed);
	}
	
	void 
	setContentSize(long size) {
		this->content_size.store(size, std::memory_order_release);
	}
	
	long 
	getContentSize(void) const {
		return this->content_size.load(std::memory_order_relaxed);
	}
		
	void *
	getBufferPointer(void) const {
		return this->buffer;
	}
	
	void *
	operator[] (const std::size_t off) const {
		return static_cast<void *>(static_cast<char *>(this->buffer) + off);
	}
}; 

class DoubleResultBuffer {
private: 
	ResultBuffer rb[2];
	std::atomic_int index;
public:
	DoubleResultBuffer(void) { this->init(); }
	~DoubleResultBuffer(void) { this->fini(); }
	
	static DoubleResultBuffer *constructor(void) {
                DoubleResultBuffer *drb = static_cast<DoubleResultBuffer *>(palloc(sizeof(*drb)));
                drb->init();
                return drb;
        }
        static void destructor(DoubleResultBuffer *drb) {
		drb->fini();
                pfree(drb);
        }
	
	void init(void) {
		this->rb[0].init();
		this->rb[1].init();
		this->index.store(0, std::memory_order_relaxed);
		std::atomic_thread_fence(std::memory_order_release);
	}
        void fini(void) {
		this->rb[0].fini();
		this->rb[1].fini();
		this->index.store(-1, std::memory_order_relaxed);
		std::atomic_thread_fence(std::memory_order_release);
	}
	
	void 
	switchResultBuffer(void) {
		this->index.store((this->index.load(std::memory_order_relaxed) + 1) % 2, std::memory_order_release);
	}
	
	void 
	changeResultBuffer(int idx) {
		this->index.store(idx, std::memory_order_release);
	}
	
	bool 
	isTerminated(void) const {
		return (this->index.load(std::memory_order_relaxed) < 0);
	}
	
	ResultBuffer *
	getCurrentResultBuffer(void) {
		return &this->rb[this->index.load(std::memory_order_relaxed)];
	}
	
	ResultBuffer *
	getNextResultBuffer(void) {
		return &this->rb[(this->index.load(std::memory_order_relaxed) + 1) % 2];
	}
};

#endif //RESULTBUFFER_HEAD_

