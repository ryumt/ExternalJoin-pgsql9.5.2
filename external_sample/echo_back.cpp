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

int main(void)
{
	int lsock, csock;
	size_t size;
	
	size_t ntup;
	Tuple *tuples;
	
	/* listen on specified port */
	lsock = listenSock(PG_PORT);
	/* accept connection from PostgreSQL */
	csock = acceptSock(lsock);
	
	/* receive total size of tuples */
	receiveStrong(csock, &size, sizeof(size));
	ntup = size / sizeof(*tuples);
	/* alloc buffer to contain tuples */
	tuples = (Tuple *)malloc(size);
	/* receive tuples */
	receiveStrong(csock, tuples, size);
		
	/* print information */
	puts("-----");
	printf("size of tuples = %zu\n", size);
	printf("size of tuple = %zu\n", sizeof(*tuples));
	printf("number of tuples = %zu\n", ntup);
	puts("-----");
	/* print tuple contents */
	for (int i = 0; i < ntup; i++)
		printf("{%d, %f}\n", tuples[i].key, tuples[i].dval);
	
	/**********************************/
	/* send back tuples twice */
	for (int j = 0; j < 2; j++)
		sendStrong(csock, tuples, sizeof(*tuples) * ntup);
	/**********************************/

	close(lsock);
	close(csock);
	
	return 0;
}
