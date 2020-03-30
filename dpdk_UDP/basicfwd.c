/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2015 Intel Corporation
 */

#include <stdint.h>
#include <inttypes.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>

#include <rte_ip.h>
#include <rte_ether.h>
#include <rte_udp.h>

#define RX_RING_SIZE 1024
#define TX_RING_SIZE 1024

#define NUM_MBUFS 8191
#define MBUF_CACHE_SIZE 250
#define BURST_SIZE 32
static const struct rte_eth_conf port_conf_default = {
    .rxmode = {
        .max_rx_pkt_len = RTE_ETHER_MAX_LEN,
    },
};

static void create_eth_ip_udp(uint8_t *msg, size_t total_len, struct rte_ether_addr dst_mac,
                              uint32_t src_ip, uint32_t dst_ip, uint16_t udp_src_port, uint16_t udp_dst_port);

struct rte_ether_addr g_dest_mac_addr;

// This will be set automatically at run time.
struct rte_ether_addr g_src_mac_addr;

static uint32_t g_src_ip = RTE_IPV4(192, 168, 174, 10);
static uint32_t g_dest_ip = RTE_IPV4(127, 0, 0, 1);
/* dpdk_UDP.c: udp send example. */

/*
 * Initializes a given port using global settings and with the RX buffers
 * coming from the mbuf_pool passed as a parameter.
 */
static inline int
port_init(uint16_t port, struct rte_mempool *mbuf_pool)
{
    struct rte_eth_conf port_conf = port_conf_default;
    const uint16_t rx_rings = 1, tx_rings = 1;
    uint16_t nb_rxd = RX_RING_SIZE;
    uint16_t nb_txd = TX_RING_SIZE;
    int retval;
    uint16_t q;
    struct rte_eth_dev_info dev_info;
    struct rte_eth_txconf txconf;

    if (!rte_eth_dev_is_valid_port(port))
        return -1;

    rte_eth_dev_info_get(port, &dev_info);
    if (dev_info.tx_offload_capa & DEV_TX_OFFLOAD_MBUF_FAST_FREE)
        port_conf.txmode.offloads |=
            DEV_TX_OFFLOAD_MBUF_FAST_FREE;

    /* Configure the Ethernet device. */
    retval = rte_eth_dev_configure(port, rx_rings, tx_rings, &port_conf);
    if (retval != 0)
        return retval;

    retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
    if (retval != 0)
        return retval;

    /* Allocate and set up 1 RX queue per Ethernet port. */
    for (q = 0; q < rx_rings; q++)
    {
        retval = rte_eth_rx_queue_setup(port, q, nb_rxd,
                                        rte_eth_dev_socket_id(port), NULL, mbuf_pool);
        if (retval < 0)
            return retval;
    }

    txconf = dev_info.default_txconf;
    txconf.offloads = port_conf.txmode.offloads;
    /* Allocate and set up 1 TX queue per Ethernet port. */
    for (q = 0; q < tx_rings; q++)
    {
        retval = rte_eth_tx_queue_setup(port, q, nb_txd,
                                        rte_eth_dev_socket_id(port), &txconf);
        if (retval < 0)
            return retval;
    }

    /* Start the Ethernet port. */
    retval = rte_eth_dev_start(port);
    if (retval < 0)
        return retval;

    /* Display the port MAC address. */
    struct rte_ether_addr addr;
    rte_eth_macaddr_get(port, &addr);
    printf("Port %u MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
           " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
           port,
           addr.addr_bytes[0], addr.addr_bytes[1],
           addr.addr_bytes[2], addr.addr_bytes[3],
           addr.addr_bytes[4], addr.addr_bytes[5]);

    /* Enable RX in promiscuous mode for the Ethernet device. */
    rte_eth_promiscuous_enable(port);

    return 0;
}
// help for debug
void print_header(uint8_t *packet_data, const unsigned len)
{
    for (int i = 0; i < len; i++)
    {
        printf("%02X", packet_data[i]);
    }
    printf("\n");
}

/*"
 * The lcore main. This is the main thread that does the work, reading from
 * an input port and writing to an output port.
 */
void lcore_main(struct rte_mempool *mbuf_pool)
{
    uint16_t port;

    /*
	 * Check that the port is on the same NUMA node as the polling thread
	 * for best performance.
	 */
    for (int i = 0; i < 2; i++)
    {
        RTE_ETH_FOREACH_DEV(port)
        {
            // The smallest packet allowed by Ethernet.
            const unsigned eth_total_len = 64;

            /* Get burst of RX packets, from first port of pair. */
            struct rte_mbuf *bufs[BURST_SIZE];

            const uint16_t nb_rx = 1;
            bufs[0] = rte_pktmbuf_alloc(mbuf_pool);
            bufs[0]->pkt_len = eth_total_len;
            bufs[0]->data_len = eth_total_len;
            uint8_t *packet_data = rte_pktmbuf_mtod(bufs[0], uint8_t *);
            if (packet_data == 0)
            {
                printf("\nrte_pktmbuf_mtod error\n");
            }

            const int UDP_PORT1 = 1234;
            const int UDP_PORT2 = 1235;
            rte_eth_macaddr_get(port, &g_dest_mac_addr);
            rte_eth_macaddr_get(port ^ 1, &g_src_mac_addr);
            create_eth_ip_udp(packet_data, eth_total_len, g_dest_mac_addr,
                              g_src_ip, g_dest_ip, UDP_PORT1, UDP_PORT2);

            /* Send burst of TX packets, to  port^1  */
            const uint16_t nb_tx = rte_eth_tx_burst(port ^ 1, 0,
                                                    bufs, nb_rx);

            /* Free any unsent packets. */
            if (unlikely(nb_tx < nb_rx))
            {
                uint16_t buf;
                for (buf = nb_tx; buf < nb_rx; buf++)
                    rte_pktmbuf_free(bufs[buf]);
            }
        }
    }
    exit(0);
}

/*
 * The main function, which does initialization and calls the per-lcore
 * functions.
 */
int main(int argc, char *argv[])
{
    struct rte_mempool *mbuf_pool;
    unsigned nb_ports;
    uint16_t portid;

    /* Initialize the Environment Abstraction Layer (EAL). */
    int ret = rte_eal_init(argc, argv);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");

    argc -= ret;
    argv += ret;

    /* Check that there is an even number of ports to send/receive on. */
    nb_ports = rte_eth_dev_count_avail();
    if (nb_ports < 2 || (nb_ports & 1))
        rte_exit(EXIT_FAILURE, "Error: number of ports must be even\n");

    /* Creates a new mempool in memory to hold the mbufs. */
    mbuf_pool = rte_pktmbuf_pool_create("MBUF_POOL", NUM_MBUFS * nb_ports,
                                        MBUF_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

    if (mbuf_pool == NULL)
        rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

    /* Initialize all ports. */
    RTE_ETH_FOREACH_DEV(portid)
    if (port_init(portid, mbuf_pool) != 0)
        rte_exit(EXIT_FAILURE, "Cannot init port %" PRIu16 "\n",
                 portid);

    if (rte_lcore_count() > 1)
        printf("\nWARNING: Too many lcores enabled. Only 1 used.\n");

    /* Call lcore_main on the master core only. */
    lcore_main(mbuf_pool);

    return 0;
}
#define DATA_LEN 64
static void create_eth_ip_udp(uint8_t *msg, size_t total_len, struct rte_ether_addr dst_mac,
                              uint32_t src_ip, uint32_t dst_ip, uint16_t udp_src_port, uint16_t udp_dst_port)
{
    // Packet looks like this:
    //   Eth  |  IP  |  UDP  |  <payload>
    // We will fill out each section in order.
    uint16_t udp_len = 64 + 8;

    struct rte_ether_hdr *eth = (struct rte_ether_hdr *)msg;

    rte_ether_addr_copy(&dst_mac, &eth->d_addr);
    rte_ether_addr_copy(&g_src_mac_addr, &eth->s_addr);
    eth->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
    print_header(msg, sizeof(struct rte_ether_hdr));

    struct rte_ipv4_hdr *ip = (struct rte_ipv4_hdr *)(eth + 1);
    size_t ip_len = total_len - sizeof(struct rte_ether_hdr);
    ip->version_ihl = 0x45;
    ip->type_of_service = 0;
    ip->total_length = rte_cpu_to_be_16(20 + (rte_be16_t)(sizeof(struct rte_udp_hdr)) + (rte_be16_t)(11));
    ip->packet_id = 1;
    ip->fragment_offset = 0;
    ip->time_to_live = 64;
    ip->next_proto_id = 0x11;
    ip->src_addr = rte_cpu_to_be_32(src_ip);
    ip->dst_addr = rte_cpu_to_be_32(dst_ip);
    ip->hdr_checksum = rte_ipv4_cksum(ip);

    print_header(ip, sizeof(struct rte_ipv4_hdr));

    struct rte_udp_hdr *udp = (struct rte_udp_hdr *)(ip + 1);

    udp->src_port = rte_cpu_to_be_16(udp_src_port);
    udp->dst_port = rte_cpu_to_be_16(udp_dst_port);
    udp->dgram_len = rte_cpu_to_be_16(8 + 3);
    udp->dgram_cksum = 0;

    print_header(udp, sizeof(struct rte_udp_hdr));

    printf("\n\n");
    // Use the packet count as the payload.
    uint32_t *payload = (uint32_t *)(udp + 1);
    payload[0] = 'a';
    payload[1] = 's';
    payload[2] = 'l';
}
