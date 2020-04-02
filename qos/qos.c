#include "qos.h"
#include <stdint.h>

#include <rte_common.h>
#include <rte_eal.h>
#include <rte_malloc.h>
#include <rte_mempool.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_mbuf.h>
#include <rte_meter.h>

#include <rte_red.h>

#define FUNC_CONFIG rte_meter_srtcm_config
#define PARAMS app_srtcm_params
#define FLOW_METER struct rte_meter_srtcm

/**
 * This function will be called only once at the beginning of the test. 
 * You can initialize your meter here.
 * 
 * int rte_meter_srtcm_config(struct rte_meter_srtcm *m, struct rte_meter_srtcm_params *params);
 * @return: 0 upon success, error code otherwise
 * 
 * void rte_exit(int exit_code, const char *format, ...)
 * #define rte_panic(...) rte_panic_(__func__, __VA_ARGS__, "dummy")
 * 
 * uint64_t rte_get_tsc_hz(void)
 * @return: The frequency of the RDTSC timer resolution
 * 
 * static inline uint64_t rte_get_tsc_cycles(void)
 * @return: The time base for this lcore.
 */
unsigned flowweight[APP_FLOWS_MAX] = {8, 4, 2, 1};
//cir = bit/s  cbs-> bits
struct rte_meter_srtcm_params app_srtcm_params[] = {
    {.cir = (int)(1.28 * (1000 ^ 3) * 8 / 15), .cbs = 2048 * 8, .ebs = 2048 * 8},
    {.cir = (int)((1.28 * (1000 ^ 3)) * 4 / 15), .cbs = 2048 * 4, .ebs = 2048 * 4},
    {.cir = (int)((1.28 * (1000 ^ 3)) * 2 / 15), .cbs = 2048 * 2, .ebs = 2048 * 2},
    {.cir = (int)((1.28 * (1000 ^ 3)) / 15), .cbs = 2048 * 1, .ebs = 2048 * 1},
};

FLOW_METER app_flows[APP_FLOWS_MAX];
int qos_meter_init(void)
{

    uint32_t i, j;
    int ret;

    for (i = 0, j = 0; i < APP_FLOWS_MAX;
         i++, j = (j + 1) % RTE_DIM(PARAMS))
    {
        printf("%d\n", j);
        ret = FUNC_CONFIG(&app_flows[i], &PARAMS[j]);
        if (ret)
            return ret;
    }
}

/**
 * This function will be called for every packet in the test, 
 * after which the packet is marked by returning the corresponding color.
 * 
 * A packet is marked green if it doesn't exceed the CBS, 
 * yellow if it does exceed the CBS, but not the EBS, and red otherwise
 * 
 * The pkt_len is in bytes, the time is in nanoseconds.
 * 
 * Point: We need to convert ns to cpu circles
 * Point: Time is not counted from 0
 * 
 * static inline enum rte_meter_color rte_meter_srtcm_color_blind_check(struct rte_meter_srtcm *m,
	uint64_t time, uint32_t pkt_len)
 * 
 * enum qos_color { GREEN = 0, YELLOW, RED };
 * enum rte_meter_color { e_RTE_METER_GREEN = 0, e_RTE_METER_YELLOW,  
	e_RTE_METER_RED, e_RTE_METER_COLORS };
 */

enum qos_color
qos_meter_run(uint32_t flow_id, uint32_t pkt_len, uint64_t time)
{
    uint8_t output_color;
    uint64_t cputime = time * 1.28 * 1000 * 1000 * 1000; //1.28 Gbps //convert ns to cpu circles
    output_color = (uint8_t)rte_meter_srtcm_color_blind_check(&app_flows[flow_id], cputime, pkt_len);

    /* Apply policing and set the output color */

    return output_color;
}

/**
 * This function will be called only once at the beginning of the test. 
 * You can initialize you dropper here
 * 
 * int rte_red_rt_data_init(struct rte_red *red);
 * @return Operation status, 0 success
 * 
 * int rte_red_config_init(struct rte_red_config *red_cfg, const uint16_t wq_log2, 
   const uint16_t min_th, const uint16_t max_th, const uint16_t maxp_inv);
 * @return Operation status, 0 success 
 */
struct rte_red *red;
struct rte_red_config *red_cfg_red[APP_FLOWS_MAX];

struct rte_red_config *red_cfg_yellow[APP_FLOWS_MAX];

struct rte_red_config *red_cfg_green[APP_FLOWS_MAX];

int qos_dropper_init(void)
{

    red = (struct rte_red *)malloc(sizeof(struct rte_red));

    const uint16_t wq_log2_red = 9; // inverse filter weight value
    const uint16_t min_th_red = 16; //threshold limit
    const uint16_t max_th_red = 32;
    const uint16_t maxp_inv_red = 10; //inverse mark probability value

    const uint16_t wq_log2_yellow = 9; // inverse filter weight value
    const uint16_t min_th_yellow = 22; // threshold limit
    const uint16_t max_th_yellow = 32;
    const uint16_t maxp_inv_yellow = 10; //inverse mark probability value

    const uint16_t wq_log2_green = 9; // inverse filter weight value
    const uint16_t min_th_green = 28; //threshold limit
    const uint16_t max_th_green = 32;
    const uint16_t maxp_inv_green = 10; //inverse mark probability value

    //  8:4:2:1, that is flow 0 has highest quality of service and its allocated bandwidth is 8 times of flow 3.

    rte_red_rt_data_init(red);
    for (int i = 0; i < APP_FLOWS_MAX; i++)
    {
        red_cfg_red[i] = (struct rte_red_config *)malloc(sizeof(struct rte_red_config));
        red_cfg_yellow[i] = (struct rte_red_config *)malloc(sizeof(struct rte_red_config));
        red_cfg_green[i] = (struct rte_red_config *)malloc(sizeof(struct rte_red_config));
        rte_red_config_init(red_cfg_red[i], wq_log2_red, min_th_red * flowweight[i], max_th_red * flowweight[i], maxp_inv_red);
        rte_red_config_init(red_cfg_yellow[i], wq_log2_yellow, min_th_yellow * flowweight[i], max_th_yellow * flowweight[i], maxp_inv_yellow);
        rte_red_config_init(red_cfg_green[i], wq_log2_green, min_th_green * flowweight[i], max_th_green * flowweight[i], maxp_inv_green);
    }

    return 0;
}

/**
 * This function will be called for every tested packet after being marked by the meter, 
 * and will make the decision whether to drop the packet by returning the decision (0 pass, 1 drop)
 * 
 * The probability of drop increases as the estimated average queue size grows
 * 
 * static inline void rte_red_mark_queue_empty(struct rte_red *red, const uint64_t time)
 * @brief Callback to records time that queue became empty
 * @param q_time : Start of the queue idle time (q_time) 
 * 
 * static inline int rte_red_enqueue(const struct rte_red_config *red_cfg,
	struct rte_red *red, const unsigned q, const uint64_t time)
 * @param q [in] updated queue size in packets   
 * @return Operation status
 * @retval 0 enqueue the packet
 * @retval 1 drop the packet based on max threshold criteria
 * @retval 2 drop the packet based on mark probability criteria
 */
unsigned q; //the current size of the packet queue (in packets)

int qos_dropper_run(uint32_t flow_id, enum qos_color color, uint64_t time)
{
    int retval;
    // the queues will be cleared (meaning all packets in the queues will be sent out)
    // at end of the time period (1000 ns

    uint64_t cputime = time * 1.28 * 1000 * 1000 * 1000;

    if (time - red->q_time > 1000)
    {
        rte_red_mark_queue_empty(red, time);
        q = 0;
    }

    if (color == RED)
        retval = rte_red_enqueue(red_cfg_red[flow_id], red, q, cputime);
    if (color == GREEN)
        retval = rte_red_enqueue(red_cfg_green[flow_id], red, q, cputime);
    if (color == YELLOW)
        retval = rte_red_enqueue(red_cfg_yellow[flow_id], red, q, cputime);

    q++;

    return retval;
}
