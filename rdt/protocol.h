
//  SELECTIVE RESEND

#ifndef _PROTOCOL_H_
#define _PROTOCOL_H_

#include "rdt_struct.h"
#include <stdlib.h>
#include <malloc.h>

#define MAX_SEQ 7
#define OVERHEAD 6

#define HEADERSIZE 2
#define TIMEOUT 0.3
#define timer_interval 0.1
#define RDT_CHECKSUM_SIZE 4

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
    bool isend;
    frame() {}

    frame(frame_kind _kind, seq_nr _seq, seq_nr _ack, char _size, char *_info,bool _isend) : kind(_kind), seq(_seq), ack(_ack), size(_size), info(_info),isend(_isend) {}
     
};
struct Node
{
    double timeout;
    seq_nr seq;
    Node *next;
    Node() {}
    Node(double _timeout, seq_nr _seq) : timeout(_timeout), seq(_seq), next(NULL)
    {
        next = NULL;
    }
};

struct message2
{
    int size;
    char *data;
    char *c; //for free
};

frame packet_to_frame(struct packet *pkt);
packet frame_to_packet(frame *frm);
bool between(seq_nr a, seq_nr b, seq_nr c);
bool checksum(struct packet *pkt);
void inc(seq_nr &n);
static unsigned int crc32(const char *buf, int size);

#endif