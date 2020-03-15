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

#include <string>

#include <exception>

void checkcontent(message *msg);
void Receiver_send(frame_kind kind, seq_nr seq, seq_nr ack, int size, char *data);
void send_to_upperlayer();

class ReceiveInfo
{
public:
    bool received;    // Whether the packet has been received.
    bool is_end;      // Whether the packet is end of a message.
    std::string data; // Data contained in the packet.
    ReceiveInfo() : received(false), is_end(false) {}
};

int last_end;

ReceiveInfo receive_content[MAX_SEQ + 1];

/* receiver initialization, called once at the very beginning */
void Receiver_Init()
{
    fprintf(stdout, "At %.2fs: receiver initializing ...\n", GetSimulationTime());
    last_end = -1;

    for (int i = 0; i < MAX_SEQ + 1; ++i)
        receive_content[i].received = false;
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
    if (f.kind == frame_kind::data && f.seq >= 0 && f.seq <= MAX_SEQ && f.size >= 1 && f.size <= PAYLOADSIZE)
    {
        if (f.seq == (last_end + 1) % (MAX_SEQ + 1) || f.seq == (last_end + 2) % (MAX_SEQ + 1))
        {
            receive_content[f.seq].data = std::string(f.info, f.size);
            receive_content[f.seq].received = true;
            receive_content[f.seq].is_end = f.isend;
            fprintf(stdout, "At %.2fs: receiver get  seq:%d,size:%d ,start with %c,end with :%c \n", GetSimulationTime(), f.seq, f.size, f.info[0], f.info[f.size - 1]);

            send_to_upperlayer();
        }
        
            Receiver_send(frame_kind::ack, 0, f.seq, 0, NULL);
    }

    //  fprintf(stdout, "At %.2fs: receiver expect frame_expected %d   \n", GetSimulationTime(), frame_expected);
}
void send_to_upperlayer()
{
    seq_nr i = (last_end + 1) % (MAX_SEQ + 1);
    int cnt = 1;
    double currenttime = GetSimulationTime();
    while (cnt++ != MAX_SEQ + 1)
    {
        if (!receive_content[i].received)
        {
            break;
        }
        if (receive_content[i].is_end)
        {
            ReceiveInfo r_info;
            message msg;
            std::string s;
            seq_nr j = (last_end + 1) % (MAX_SEQ + 1);
            fprintf(stdout, "At %.2fs: receiver  seq start with %d,seq end with :%d \n", GetSimulationTime(), j, i);
            fprintf(stdout, "At %.2fs: receiver  seq %d received %d\n", GetSimulationTime(), j, receive_content[j].received);
            while (j != i)
            {
                receive_content[j].received = false;
                r_info = receive_content[j];
                s += r_info.data;
                inc(j);
            }

            receive_content[i].received = false;
            receive_content[i].is_end = false;

            r_info = receive_content[i];
            s += r_info.data;

            msg.size = s.size();
            msg.data = (char *)(long)s.c_str();

            // debug
            // checkcontent(&msg);
            Receiver_ToUpperLayer(&msg);
            last_end = i;
            break;
        }
        inc(i);
    }
}
void Receiver_send(frame_kind kind, seq_nr seq, seq_nr ack, int size, char *data)
{
    // transform frame to packet

    fprintf(stdout, "At %.2fs: receiver send ack %d \n", GetSimulationTime()), ack;
    frame *f = new frame(kind, seq, ack, size, data, false);
    packet p = frame_to_packet(f);
    Receiver_ToLowerLayer(&p);
}

//debug
void checkcontent(message *msg)
{
    static char cnt = 0;

    for (int i = 0; i < msg->size; i++)
    {
        /* message verification */
        if (msg->data[i] != '0' + cnt)
        {
            fprintf(stdout, "At %.2fs: receiver content err ,index:%d ,message size %d ,should be %d,err:%c\n", GetSimulationTime(), i, msg->size, cnt, msg->data[i]);
            exit(0);
        }
        cnt = (cnt + 1) % 10;
    }
}
