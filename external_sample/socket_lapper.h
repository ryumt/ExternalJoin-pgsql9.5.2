#ifndef SOCKETLAPPER_HEAD_
#define SOCKETLAPPER_HEAD_

static inline
int
setOptionSock(const int sock) {
	int optval = 1;

        if (::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
		std::fprintf(stderr, "error in setOptionSock::setsockopt()\n");
                return -1;
        }
        return 0;
}

static inline 
int
listenSock(const int port) {
        int sock;
        static const int BACKLOG = 30;
        struct sockaddr_in addr;
        
        ::bzero((char *)&addr, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);

        if ((sock = ::socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		std::fprintf(stderr, "error in listenSock::socket()\n");
                return -1;
        }
        if (setOptionSock(sock) < 0)
                return -1;
        if (::bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		std::fprintf(stderr, "error in listenSock::bind()\n");
                return -1;
        }
        if (::listen(sock, BACKLOG) < 0) {
		std::fprintf(stderr, "error in listenSock::listen()\n");
                return -1;
        }
        return sock;
}

static inline
int
acceptSock(const int server_sock) {
	int client_sock;
        struct sockaddr_in addr;
        socklen_t addr_size = sizeof(addr);

        client_sock = ::accept(server_sock, (struct sockaddr *)&addr, &addr_size);
        if (client_sock < 0) {
		std::fprintf(stderr, "error in acceptSock::accept()\n");
                return -1;
        }
        return client_sock;
}

static inline
int 
connectSock(char *ip, const int port)
{
        int sock;
        struct sockaddr_in addr;
        static socklen_t addr_size = sizeof(addr);
	
        if ((sock = ::socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		std::fprintf(stderr, "error in connectSock::sock()\n");
                return -1;
	}

        ::bzero((char *)&addr, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = ::inet_addr(ip);
        addr.sin_port = htons(port);
        if (::connect(sock, (struct sockaddr *)&addr, addr_size) < 0) {
		std::fprintf(stderr, "error in connectSock::connect()\n");
                return -1;
	}
        return sock;
}

static inline 
long 
sendStrong(const int sock, void *data, const long size)
{
        long send_byte;
        long cumulative_byte = 0;
	
	for (;;) {
                send_byte = ::send(sock, static_cast<void *>(static_cast<char *>(data) + cumulative_byte), 
				   size - cumulative_byte, 0);
		if (send_byte <= 0) {
			if (cumulative_byte > 0)
				break;
			else
				return send_byte;
		}
		cumulative_byte += send_byte;
		if (cumulative_byte == size)
			break;
        }
	return cumulative_byte;
}

static inline 
long 
receiveStrong(const int sock, void *buf, const long size)
{
        long rec_byte;
        long cumulative_byte = 0;
	
	for (;;) {
                rec_byte = ::recv(sock, static_cast<void *>(static_cast<char *>(buf) + cumulative_byte), 
				  size - cumulative_byte, 0);
		if (rec_byte <= 0) {
			if (cumulative_byte > 0)
				break;
			else
				return rec_byte;
		}
		cumulative_byte += rec_byte;
		if (cumulative_byte == size)
			break;
	}
        return cumulative_byte;
}

#endif//SOCKETLAPPER_HEAD_
