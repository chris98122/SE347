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

#define MSGBUF_SIZE 1000

seq_nr next_frame_to_send;
seq_nr ack_expected;
frame *window[MAX_SEQ + 1];
std::queue<message2 *> msgbuffer;

Node *virtual_timer;
bool windowfull = false;
bool ack_add = false;
void send(frame *f, seq_nr seq);
void msgbuffer_push_back(struct message *msg);
void msgbuffer_pop_front();
void send_when_window_available();
void window_delete(seq_nr seq);
bool delete_timeout_node(seq_nr frame_seq);
bool window_isfull();

// timer api
bool in_timeout_node(seq_nr frame_seq);
void cleartimer();
void Sender_StopTimer(seq_nr frame_seq);
void Sender_StartTimer(double timeout, seq_nr frame_seq);

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

    // initialize a virtual timer ,a dummy node
    virtual_timer = new Node(0, 0);

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

    //print seq that not delivered
    for (int i = 0; i < MAX_SEQ; i++)
    {
        if (window[i] != 0)
            fprintf(stdout, "%d", i);
    }
}

/* event handler, called when a message is passed from the upper layer at the 
   sender */
void Sender_FromUpperLayer(struct message *msg)
{

    // fprintf(stdout, "At %.2fs: Sender_FromUpperLayer\n", GetSimulationTime());
    //debug
    if (window_isfull())
    {
        fprintf(stdout, "At %.2fs: sender window is full \n", GetSimulationTime());

        fprintf(stdout, "At %.2fs: Sender_Timeout expect seq:%d\n", GetSimulationTime(), ack_expected);
        if (Sender_isTimerSet())
        {
            fprintf(stdout, "At %.2fs: sender time set expect seq:%d\n", GetSimulationTime(), ack_expected);
        }
        else
        {
            Sender_StartTimer(0);
        }
    }
    // put msg into additional buffer
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
    if (f.kind == frame_kind::ack)
    {
        //ack n implies n-1 n-2 etc
        if (!between(ack_expected, f.ack, next_frame_to_send))
        {
            fprintf(stdout, "At %.2fs: sender get wrong ack %d，ack_expected:%d,next_frame_to_send:%d  \n", GetSimulationTime(), f.ack, ack_expected, next_frame_to_send);
        }
        while (between(ack_expected, f.ack, next_frame_to_send))
        {
            window_delete(ack_expected);
            Sender_StopTimer(ack_expected);
            //debug
            //fprintf(stdout, "At %.2fs: sender get ack %d  \n", GetSimulationTime(), f.ack);
            inc(ack_expected);
        }
    }

    // if the sender window not full send the additonal buffer
    send_when_window_available();
}

/* event handler, called when the timer expires */
void Sender_Timeout()
{
    Sender_StopTimer();
    cleartimer();
    // fprintf(stdout, "At %.2fs: Sender_Timeout expect seq:%d\n", GetSimulationTime(), ack_expected);

    next_frame_to_send = ack_expected;
    for (int i = 0; i < MAX_SEQ + 1; i++)
    {
        if (window[next_frame_to_send] != 0)
        {
            send(window[next_frame_to_send], next_frame_to_send);
            fprintf(stdout, "At %.2fs: sender resend seq:%d\n", GetSimulationTime(), next_frame_to_send);
            inc(next_frame_to_send);
        }
        else
        {
            break;
            fprintf(stdout, "At %.2fs: sender window is 0 :%d\n", GetSimulationTime(), next_frame_to_send);
        }
    }
}

// virtual timer api
void cleartimer()
{
    Node *cur = virtual_timer->next;
    while (cur)
    {
        delete (cur);
        cur = cur->next;
    }
    virtual_timer->next = NULL;
}
void Sender_StartTimer(double timeout, seq_nr frame_seq)
{
    if (Sender_isTimerSet())
    {
        return;
    }
    else
    {
        Node *cur = virtual_timer;
        while (cur->next)
        {
            cur = cur->next;
        }
        double t = GetSimulationTime() + timeout;
        cur->next = new Node(t, frame_seq);
        ASSERT(cur->next->next == 0);

        //  fprintf(stdout, "At %.2fs: sender store timer %d %e\n", GetSimulationTime(), frame_seq, t);
        Sender_StartTimer(0.3);
    }
}

void Sender_StopTimer(seq_nr frame_seq)
{
    if (virtual_timer->next && virtual_timer->next->seq == frame_seq && virtual_timer->next->next == NULL)
    {
        Sender_StopTimer();
        //the only node STOP TIMER NOW
        delete (virtual_timer->next);

        virtual_timer->next = NULL;
        //  fprintf(stdout, "At %.2fs: sender stop timer %d\n", GetSimulationTime(), frame_seq);
    }
    else if (virtual_timer->next)
    {
        //delete the stopped one
        if (delete_timeout_node(frame_seq) && virtual_timer->next)
        {
            double t = GetSimulationTime() - virtual_timer->next->timeout;
            if (t > 0.0 && t < 0.3)
            {
                Sender_StartTimer(t);
            }
            else
            {
                Sender_Timeout();
            }
        }

        //  fprintf(stdout, "At %.2fs: sender start timer %d timeout:%e\n", GetSimulationTime(), frame_seq, t);
    }
}
//debug
bool in_timeout_node(seq_nr frame_seq)
{
    Node *cur = virtual_timer->next;
    while (cur && cur->next)
    {
        if (cur->next->seq == frame_seq)
        {
            return true;
        }
        cur = cur->next;
    }
    return false;
}
bool delete_timeout_node(seq_nr frame_seq)
{
    Node *cur = virtual_timer;
    while (cur && cur->next)
    {
        if (cur->next->seq == frame_seq)
        {
            Node *n = cur->next->next;
            delete (cur->next);
            cur->next = n;
            return true;
        }
        cur = cur->next;
    }
    fprintf(stdout, "At %.2fs: delete_timeout_node %d cant find\n", GetSimulationTime(), frame_seq);
    return false;
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
    delete (window[seq]);
    window[seq] = 0;
    // fprintf(stdout, "At %.2fs: sender delete window %d\n", GetSimulationTime(), seq);
}

bool window_isfull()
{
    for (int i = 0; i < MAX_SEQ + 1; i++)
    {
        if (window[i] == 0)
            return false;
    }
    return true;
}

void send_when_window_available()
{
    while (!window_isfull())
    {
        if (msgbuffer.empty())
            return;
        message2 *msg = msgbuffer.front();
        unsigned int sendsize;
        char *d;
        if (msg->size <= PAYLOADSIZE)
        {
            sendsize = msg->size;
            d = (char *)malloc(sendsize);
            // fprintf(stdout, "sender message size %d\n", msg->size);
            memcpy(d, msg->data, sendsize);
            msgbuffer_pop_front();
        }
        else
        {

            sendsize = PAYLOADSIZE;
            d = (char *)malloc(sendsize);
            memcpy(d, msg->data, sendsize);
            msg->data += PAYLOADSIZE;
            msg->size -= sendsize;
        }

        frame *f = new frame(frame_kind::data, next_frame_to_send, 0, sendsize, d);
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
    // set timer

    //debug
    Sender_StartTimer(0.3, seq);

    // fprintf(stdout, "At %.2fs: sender send seq:%d ", GetSimulationTime(), f->seq);
}