
//  go back n

#ifndef _PROTOCOL_H_
#define _PROTOCOL_H_

#include "rdt_struct.h"
#include <stdlib.h>
#define MAX_SEQ 7
#define OVERHEAD 3

#define PAYLOADSIZE RDT_PKTSIZE - OVERHEAD

typedef unsigned int seq_nr; /* sequence or ack numbers */
typedef enum
{
    data,
    ack,
    nak
} frame_kind;

struct frame
{
    frame_kind kind; /* what kind of frame */
    seq_nr seq;      /* sequence number */
    seq_nr ack;      /* acknowledgement number */
    char size;
    char *info; /* the network layer packet */
    frame() {}

    frame(frame_kind _kind, seq_nr _seq, seq_nr _ack, char _size, char *_info) : kind(_kind), seq(_seq), ack(_ack), size(_size), info(_info) {}
};
struct Node
{
    double timeout;
    seq_nr seq;
    Node *next;
    Node(){}
    Node(double _timeout, seq_nr _seq) : timeout(_timeout), seq(_seq), next(NULL) {}
};

frame packet_to_frame(struct packet *pkt);
packet frame_to_packet(frame *frm);
bool between(seq_nr a, seq_nr b, seq_nr c);
bool checksum(struct packet *pkt);
unsigned char CRC8Calculate(void *pBuf, unsigned pBufSize);
void inc(seq_nr &n);

#endif