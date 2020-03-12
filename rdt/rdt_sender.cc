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

#define MSGBUF_SIZE 10

seq_nr next_frame_to_send;
seq_nr ack_expected;
frame *window[MAX_SEQ + 1];
message *msgbuffer[MSGBUF_SIZE];

Node *virtual_timer;

void send(frame *f, seq_nr seq);
void msgbuffer_push_back(struct message *msg);
void msgbuffer_pop_front();
void send_when_window_available();
void window_delete(seq_nr seq);
void Sender_StopTimer(seq_nr frame_seq);
void Sender_StartTimer(double timeout, seq_nr frame_seq);
seq_nr get_timeout_frame_num();
void delete_timeout_node(seq_nr frame_seq);

/* sender initialization, called once at the very beginning */
void Sender_Init()
{
    fprintf(stdout, "At %.2fs: sender initializing ...\n", GetSimulationTime());
    fprintf(stdout, "PAYLOADSIZE %d", PAYLOADSIZE);
    // initiallize seq num
    ack_expected = 0;
    next_frame_to_send = 0;
    // initialize sender buffer
    memset(window, 0, sizeof(frame *) * (MAX_SEQ + 1));

    // initialize a virtual timer ,a dummy node
    virtual_timer = new Node(0, 0);

    // initialize additional buffer
    memset(msgbuffer, 0, sizeof(message *) * MSGBUF_SIZE);
}

/* sender finalization, called once at the very end.
   you may find that you don't need it, in which case you can leave it blank.
   in certain cases, you might want to take this opportunity to release some 
   memory you allocated in Sender_init(). */
void Sender_Final()
{
    fprintf(stdout, "At %.2fs: sender finalizing ...\n", GetSimulationTime());

    //print seq that not delivered
    for(int i = 0;i<MAX_SEQ;i++)
    {
        if(window[i] !=0)
            fprintf(stdout,"%d",i);
    }
    
}

/* event handler, called when a message is passed from the upper layer at the 
   sender */
void Sender_FromUpperLayer(struct message *msg)
{

    // fprintf(stdout, "At %.2fs: Sender_FromUpperLayer\n", GetSimulationTime());
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
            //debug
            Sender_StopTimer(ack_expected);
            //fprintf(stdout, "At %.2fs: sender get ack %d  \n", GetSimulationTime(), f.ack);
            window_delete(ack_expected);
            inc(ack_expected);
        }
    }
    else
    { // data type-> ?
        // nak type ->
        ASSERT(0);
    }

    // if the sender window not full send the additonal buffer
    send_when_window_available();
}

/* event handler, called when the timer expires */
void Sender_Timeout()
{
    // get to know the timeout frame number
    seq_nr seq = get_timeout_frame_num();

    // get the frame from sender window

    fprintf(stdout, "At %.2fs: sender resend seq:%d\n", GetSimulationTime(), seq);
    frame *f = window[seq];
    ASSERT(seq >= 0 && seq <= MAX_SEQ);

    if (f == 0)
        return;

    delete_timeout_node(seq);

    // resend it
    send(f, seq);
}

// virtual timer api
void Sender_StartTimer(double timeout, seq_nr frame_seq)
{
    // append a node
    if (virtual_timer->next == NULL)
    {
        //the only node START TIMER NOW
        virtual_timer->next = new Node(timeout, frame_seq);
        Sender_StartTimer(timeout);
    }
    else
    {
        Node *cur = virtual_timer;
        while (cur->next)
        {
            cur = cur->next;
        }
        cur->next = new Node(GetSimulationTime() + timeout, frame_seq);
    }

  //  fprintf(stdout, "At %.2fs: sender start timer %d\n", GetSimulationTime(), frame_seq);
}

void Sender_StopTimer(seq_nr frame_seq)
{
    if (virtual_timer->next->next == NULL)
    {
        //the only node STOP TIMER NOW
        delete (virtual_timer->next);
        Sender_StopTimer();
    }
    else
    {
        //delete the stopped one
        delete_timeout_node(frame_seq);
        double timeout = virtual_timer->next->timeout;
        Sender_StartTimer(GetSimulationTime() - timeout);
    }
  //  fprintf(stdout, "At %.2fs: sender stop timer %d\n", GetSimulationTime(), frame_seq);
}

void delete_timeout_node(seq_nr frame_seq)
{
    Node *cur = virtual_timer;
    while (cur && cur->next)
    {
        if (cur->next->seq == frame_seq)
        {
            Node *n = cur->next->next;
            delete (cur->next);
            cur->next = n;
        }
        cur = cur->next;
    }
}
seq_nr get_timeout_frame_num()
{
    ASSERT(virtual_timer->next);
    return virtual_timer->next->seq;
}

// msgbuffer api
void msgbuffer_push_back(struct message *msg)
{
    message *m = (message *)malloc(sizeof(message *));
    char *c = (char *)malloc(sizeof(char) * (msg->size));
    memcpy(c, msg->data, msg->size);
    m->size = msg->size;
    m->data = c;
    for (int i = 0; i < MSGBUF_SIZE; i++)
    {
        if (msgbuffer[i] == 0)
        {
            msgbuffer[i] = m;
            // fprintf(stdout, "At %.2fs: msgbuffer push %d\n", GetSimulationTime(), i);
            return;
        }
    }
    // debug
    fprintf(stdout, "At %.2fs: sender MSGBUF is full\n", GetSimulationTime());
    exit(0);
}
void msgbuffer_pop_front()
{
    free(msgbuffer[0]);
    for (int i = 0; i < MSGBUF_SIZE - 1; i++)
    {
        msgbuffer[i] = msgbuffer[i + 1];
    }
    msgbuffer[MSGBUF_SIZE - 1] = 0;

    //  fprintf(stdout, "At %.2fs: sender MSGBUF msgbuffer_pop_front\n", GetSimulationTime());
}

// window api
void window_add(frame *frm, seq_nr seq)
{
    window[seq] = frm;
}

void window_delete(seq_nr seq)
{
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
        message *msg = msgbuffer[0];
        if (msg == 0) //no msg
            break;
        char *d = msg->data;
        unsigned int sendsize;
        if (msg->size <= PAYLOADSIZE)
        {
            sendsize = msg->size;
            msgbuffer_pop_front();
        }
        else
        {

            //    fprintf(stdout, "sender message size %d\n", msg->size);
            sendsize = PAYLOADSIZE;
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
    packet p = frame_to_packet(f);

    // send it
    Sender_ToLowerLayer(&p);
    // set timer

    //debug
    Sender_StartTimer(0.3, seq);

    // fprintf(stdout, "At %.2fs: sender send seq:%d ", GetSimulationTime(), f->seq);
}