#include "protocol.h"
#include "rdt_struct.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cstring>

frame packet_to_frame(struct packet *pkt)
{
    frame f;
    char *d = pkt->data;
    f.kind = frame_kind(d[0] & 0b11);
    f.seq = (d[0] & 0b11100) >> 2;
    f.ack = (d[0] & 0b11100000) >> 5;
    f.size = d[1];
    f.info = d + 2;
    return f;
}

packet frame_to_packet(frame *frm)
{
    struct packet p;
    p.data[0] = frm->kind;      //00 = data, 01=ack, 10 = nak 2-bit
    p.data[0] += frm->seq << 2; // 000-111 3-bit
    p.data[0] += frm->ack << 5; // 000-111 3-bit
    p.data[1] = frm->size;
    // fprintf(stdout, "p.data: %llu \n", p.data);

    // fprintf(stdout, "frm->info: %llu \n", frm->info);

    // fprintf(stdout, "frm->size: %llu \n", frm->size);
    memcpy(p.data + 2, frm->info, frm->size);
    p.data[RDT_PKTSIZE - 1] = CRC8Calculate(p.data, RDT_PKTSIZE - 1);
    return p;
}

bool between(seq_nr a, seq_nr b, seq_nr c) // a is never equal to c
{
    //return true if a<=b<c circularly ,else return false
     return (((a <= b) && (b < c)) || ((c < a) && (a <= b)) || ((b < c) && (c < a)) || ((a == b) && (b==c)));
}

bool checksum(struct packet *pkt)
{
    return CRC8Calculate(pkt->data, RDT_PKTSIZE) == 0;
}

unsigned char CRC8Calculate(void *pBuf, unsigned pBufSize)
{
    unsigned char retCRCValue = 0x00;
    unsigned char *pData;
    int i = 0;
    unsigned char pDataBuf = 0;
    //  retCRCValue=0x01;
    pData = (unsigned char *)pBuf;
    // pDataBuf=pData[0];
    // cout<<hex<<pDataBuf<<endl;

    while (pBufSize--)
    {
        pDataBuf = *pData++;
        for (i = 0; i < 8; i++)
        {
            if ((retCRCValue ^ (pDataBuf)) & 0x01)
            {
                retCRCValue ^= 0x18;
                retCRCValue >>= 1;
                retCRCValue |= 0x80;
                //    printf("i=%d;retCRCValue=%x\n",i,retCRCValue);
            }
            else
            {
                retCRCValue >>= 1;
                //     printf("i=%d;retCRCValue=%x\n",i,retCRCValue);
            }
            pDataBuf >>= 1;
        }
    }
    return retCRCValue;
}

void inc(seq_nr &n)
{
    n = (n + 1) % (MAX_SEQ + 1);
}