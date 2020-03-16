/*
 * FILE: rdt_sender.cc
 * DESCRIPTION: Reliable data transfer sender.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rdt_struct.h"
#include "rdt_sender.h"

#include "protocol.h"

#include <queue>

seq_nr next_frame_to_send;
seq_nr ack_expected;
frame *window[MAX_SEQ + 1];
std::queue<message2 *> msgbuffer;

double sendtime[MAX_SEQ + 1];

void send(frame *f, seq_nr seq);
void msgbuffer_push_back(struct message *msg);
void msgbuffer_pop_front();
void send_when_window_available();
void window_delete(seq_nr seq);
bool delete_timeout_node(seq_nr frame_seq); 
bool window_is_empty();
void window_add(frame *frm, seq_nr seq);

/* sender initialization, called once at the very beginning */
void Sender_Init()
{
    fprintf(stdout, "At %.2fs: sender initializing ...\n", GetSimulationTime());
    // fprintf(stdout, "PAYLOADSIZE %d", PAYLOADSIZE);
    // initiallize seq num
    ack_expected = 0;
    next_frame_to_send = 0;
    // initialize sender buffer
    memset(window, 0, sizeof(frame *) * (MAX_SEQ + 1));

    // initialize additional buffer
    msgbuffer = std::queue<message2 *>();
}

/* sender finalization, called once at the very end.
   you may find that you don't need it, in which case you can leave it blank.
   in certain cases, you might want to take this opportunity to release some 
   memory you allocated in Sender_init(). */
void Sender_Final()
{
    fprintf(stdout, "At %.2fs: sender finalizing ...\n", GetSimulationTime());
}

/* event handler, called when a message is passed from the upper layer at the 
   sender */
void Sender_FromUpperLayer(struct message *msg)
{

    msgbuffer_push_back(msg);

    send_when_window_available();
}

/* event handler, called when a packet is passed from the lower layer at the 
   sender */
void Sender_FromLowerLayer(struct packet *pkt)
{
    // wrong checksum -> just drop it
    if (!checksum(pkt))
    {
        fprintf(stdout, "At %.2fs: sender checksum err\n", GetSimulationTime());
        return;
    }

    // transform packet to frame
    frame f = packet_to_frame(pkt);

    // check frame type
    if (f.kind == frame_kind::ack && f.ack >= 0 && f.ack <= MAX_SEQ)
    {
        //ack n implies n-1 n-2 etc
        // fprintf(stdout, "At %.2fs: sender get ack %d  \n", GetSimulationTime(), f.ack);
        window_delete(f.ack);
    }

    // if the sender window not full send the additonal buffer
    send_when_window_available();
}

/* event handler, called when the timer expires */
void Sender_Timeout()
{
    // fprintf(stdout, "At %.2fs: Sender_Timeout \n", GetSimulationTime());
    double currenttime = GetSimulationTime();
    for (int i = 0; i < MAX_SEQ + 1; i++)
    {
        if (window[i] != 0 && currenttime - sendtime[i] >= TIMEOUT)
        {
            // fprintf(stdout, "At %.2fs: sender resend seq:%d\n", GetSimulationTime(), i);
            send(window[i], i);
        }
        else if (!window_is_empty())
        {
            Sender_StartTimer(timer_interval);
        }
    }
}

// msgbuffer api
void msgbuffer_push_back(struct message *msg)
{
    message2 *m = (message2 *)malloc(sizeof(message2));
    char *c = (char *)malloc(sizeof(char) * (msg->size));
    memcpy(c, msg->data, msg->size);
    m->size = msg->size;
    m->data = c;
    m->c = c;
    msgbuffer.push(m);
}
void msgbuffer_pop_front()
{
    if (msgbuffer.empty())
        return;
    if (msgbuffer.front()->c != NULL)
        free(msgbuffer.front()->c);
    free(msgbuffer.front());
    msgbuffer.pop();
    //    fprintf(stdout, "At %.2fs: sender MSGBUF msgbuffer_pop_front\n", GetSimulationTime());
}

// window api
void window_add(frame *frm, seq_nr seq)
{
    window[seq] = frm;
}

void window_delete(seq_nr seq)
{
    if (window[seq] == 0)
        return;
    delete (window[seq]->info);
    delete (window[seq]);
    window[seq] = 0;
    // fprintf(stdout, "At %.2fs: sender delete window %d\n", GetSimulationTime(), seq);
}
 
bool window_is_empty()
{
    for (int i = 0; i < MAX_SEQ + 1; i++)
    {
        if (window[i] != 0)
            return false;
    }
    return true;
}
void send_when_window_available()
{
    if (!window_is_empty())
        return;
    while (true)
    {
        if (msgbuffer.empty())
            return;
        message2 *msg = msgbuffer.front();
        unsigned int sendsize;
        char *d;
        frame *f;
        if (msg->size <= PAYLOADSIZE)
        {
            sendsize = msg->size;
            d = (char *)malloc(sendsize);
            // fprintf(stdout, "sender message size %d\n", msg->size);
            memcpy(d, msg->data, sendsize);
            f = new frame(frame_kind::data, next_frame_to_send, 0, sendsize, d, true);
            msgbuffer_pop_front();
            window_add(f, next_frame_to_send);
            send(f, next_frame_to_send);
            inc(next_frame_to_send);
            break;
        }
        else
        {
            sendsize = PAYLOADSIZE;
            d = (char *)malloc(sendsize);
            memcpy(d, msg->data, sendsize);
            f = new frame(frame_kind::data, next_frame_to_send, 0, sendsize, d, false);
            msg->data += PAYLOADSIZE;
            msg->size -= sendsize;
        }

        window_add(f, next_frame_to_send);
        send(f, next_frame_to_send);
        inc(next_frame_to_send);
    }
}

// send api
void send(frame *f, seq_nr seq)
{
    // transform frame to packet
    packet p;
    p = frame_to_packet(f);

    // send it
    Sender_ToLowerLayer(&p);

    sendtime[seq] = GetSimulationTime();

    if (!Sender_isTimerSet())
        Sender_StartTimer(TIMEOUT);

    // fprintf(stdout, "At %.2fs: sender content  seq:%d,size:%d,start with %c,end with :%c \n", GetSimulationTime(), seq, f->size, f->info[0], f->info[f->size - 1]);

    // fprintf(stdout, "At %.2fs: sender send seq:%d \n", GetSimulationTime(), f->seq);
}