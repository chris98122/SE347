/*
 * FILE: rdt_receiver.cc
 * DESCRIPTION: Reliable data transfer receiver.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rdt_struct.h"
#include "rdt_receiver.h"

#include "protocol.h"

void Receiver_send(frame_kind kind, seq_nr seq, seq_nr ack, int size, char *data);
seq_nr frame_expected;

/* receiver initialization, called once at the very beginning */
void Receiver_Init()
{
    fprintf(stdout, "At %.2fs: receiver initializing ...\n", GetSimulationTime());

    // initialize seq num
    frame_expected = 0;
}

/* receiver finalization, called once at the very end.
   you may find that you don't need it, in which case you can leave it blank.
   in certain cases, you might want to use this opportunity to release some 
   memory you allocated in Receiver_init(). */
void Receiver_Final()
{
    fprintf(stdout, "At %.2fs: receiver finalizing ...\n", GetSimulationTime());
}

/* event handler, called when a packet is passed from the lower layer at the 
   receiver */
void Receiver_FromLowerLayer(struct packet *pkt)
{
    // wrong checksum -> just drop it
    if (!checksum(pkt))
    {
        fprintf(stdout, "At %.2fs: receiver checksum err\n", GetSimulationTime());
        return;
    }

    // transform packet to frame
    frame f = packet_to_frame(pkt);

    // data type-> Receiver_send() ACK
    if (f.kind == frame_kind::data)
    {
        //check frame seq
        if (f.seq == frame_expected)
        {
            message msg;
            msg.size = f.size;
            msg.data = f.info;

            fprintf(stdout, "At %.2fs: receiver get  seq:%d \n", GetSimulationTime(), frame_expected);

            Receiver_ToUpperLayer(&msg);
            inc(frame_expected);
            Receiver_send(frame_kind::ack, 0, f.seq, 0, NULL);
        }
        else
        {
            fprintf(stdout, "At %.2fs: receiver get unexpected seq:%d,size:%d\n", GetSimulationTime(), f.seq, f.size);
        }
    }
}

void Receiver_send(frame_kind kind, seq_nr seq, seq_nr ack, int size, char *data)
{
    // transform frame to packet
    frame *f = new frame(kind, seq, ack, size, data);
    packet p = frame_to_packet(f);
    Receiver_ToLowerLayer(&p);
}