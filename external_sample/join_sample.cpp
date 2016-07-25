#include <cstdio>
#include <cstdlib>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "socket_lapper.h"

/* this can be modified */
#define PG_PORT (59999)

/****** modify this... ******/
struct Tuple {
	int key; 
	double dval;
};
/****** modify this... ******/
struct Result {
	int key1;
	double dval1;
	int key2;
	double dval2;
};


int main(void)
{
	int lsock, csock;
		
	size_t size[2];
	size_t ntup[2];
	Tuple *tuples[2];
	
	/* listen on specified port */
	lsock = listenSock(PG_PORT);
	/* accept connection from PostgreSQL */
	csock = acceptSock(lsock);
	
	for (int i = 0; i < 2; i++) {
		/* receive total size of tuples */
		receiveStrong(csock, &size[i], sizeof(size[0]));
		ntup[i] = size[i] / sizeof(*tuples[0]);
		/* alloc buffer to contain tuples */
		tuples[i] = (Tuple *)malloc(size[i]);
		/* receive tuples */
		receiveStrong(csock, tuples[i], size[i]);
	}
	
	
	/******** nest loop join ********/
	/* SELECT * FROM t1, t2 WHERE (t1.dval - t2.dval)^2 < 10; */
	for (int i = 0; i < ntup[0]; i++) {
		for (int j = 0; j < ntup[1]; j++) {
			double diff = tuples[0][i].dval - tuples[1][j].dval;
			
			if (diff * diff < 10) {
				Result result;
				
				result.key1 = tuples[0][i].key;
				result.dval1 = tuples[0][i].dval;
				result.key2 = tuples[1][j].key;
				result.dval2 = tuples[1][j].dval;
				
				sendStrong(csock, &result, sizeof(result));
			}
		}
	}
	/*******************************/
	
	close(lsock);
	close(csock);
	
	return 0;
}

