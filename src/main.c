/*
 * Zephyr Ethernet Stress Test
 *
 * Performs HTTP GET requests and MQTT operations (publish/subscribe)
 * in separate threads to stress the Ethernet connection.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/posix/poll.h>
#include <zephyr/net/net_if.h>
#include <zephyr/net/net_mgmt.h>
#include <zephyr/net/ethernet_mgmt.h>
#include <zephyr/net/net_event.h>
#include <zephyr/net/dhcpv4.h>
#include <zephyr/net/socket.h>
#include <zephyr/net/http/client.h>
#include <zephyr/net/mqtt.h>
#include <zephyr/net/net_ip.h>
#include <zephyr/random/random.h>
#include <zephyr/drivers/hwinfo.h>
#include <string.h>
#include <errno.h>

LOG_MODULE_REGISTER(eth_stress, LOG_LEVEL_INF);

/* ========================================================================== */
/*  Configuration                                                             */
/* ========================================================================== */

/* Target HTTP server (e.g., a local or public web server) */
#define HTTP_SERVER_HOST   "viacep.com.br"   /* Replace with the HTTP server IP */
#define HTTP_SERVER_PORT   80
#define HTTP_PATH          "/ws/01001000/json/"

/* MQTT broker */
#define MQTT_BROKER_HOST   "broker.emqx.io"   /* Replace with the MQTT broker IP */
#define MQTT_BROKER_PORT   1883
#define MQTT_TOPIC_SUB     "stress/test"
#define MQTT_TOPIC_PUB     "stress/data"
#define MQTT_PUB_INTERVAL  1000   /* ms between publishes */
#define MQTT_SUB_INTERVAL  2000   /* ms between message checks */

/* Interval between HTTP requests (ms) */
#define HTTP_INTERVAL      3000

/* Buffer sizes */
#define HTTP_RECV_BUF_SIZE 512
#define MQTT_RX_BUF_SIZE   4096
#define MQTT_TX_BUF_SIZE   512

/* ========================================================================== */
/*  Global Variables                                                          */
/* ========================================================================== */

static K_SEM_DEFINE(net_ready, 0, 1);
static struct net_mgmt_event_callback net_mgmt_cb;
static struct net_mgmt_event_callback ethernet_mgmt_cb;

static struct mqtt_client mqtt_client;
static struct sockaddr_storage mqtt_broker;
static uint8_t mqtt_rx_buffer[MQTT_RX_BUF_SIZE];
static uint8_t mqtt_tx_buffer[MQTT_TX_BUF_SIZE];
static bool mqtt_connected = false;
static char mqtt_client_id[64];

/* ========================================================================== */
/*  Network event callback                                                    */
/* ========================================================================== */

static void net_event_handler(struct net_mgmt_event_callback *cb,
                              uint64_t mgmt_event,
                              struct net_if *iface)
{
    switch (mgmt_event) {
        case NET_EVENT_IPV4_ADDR_ADD:
            char addr_str[NET_IPV4_ADDR_LEN];
            const struct in_addr *addr =
                &iface->config.ip.ipv4->unicast[0].ipv4.address.in_addr;

            net_addr_ntop(NET_AF_INET, addr, addr_str, sizeof(addr_str));
            LOG_INF("IPv4 address obtained: %s", addr_str);

            k_sem_reset(&net_ready);
        break;

        case NET_EVENT_IPV4_ADDR_DEL:
            net_dhcpv4_restart(iface);
        break;

        default:
        break;
    }
}

static void ethernet_event_handler(struct net_mgmt_event_callback *cb,
                              uint64_t mgmt_event,
                              struct net_if *iface)
{
    switch (mgmt_event) {
        case NET_EVENT_ETHERNET_CARRIER_ON:
            LOG_INF("Ethernet carrier ON (cable connected)");

            net_dhcpv4_restart(iface);
        break;

        case NET_EVENT_ETHERNET_CARRIER_OFF:
            LOG_WRN("Ethernet carrier OFF (cable disconnected)");
        break;
    }
}

static int http_response_cb(struct http_response *rsp,
                            enum http_final_call final_data,
                            void *user_data)
{
    ARG_UNUSED(user_data);

    if (final_data == HTTP_DATA_MORE) {
        LOG_INF("HTTP: partial data received (%zd bytes)", rsp->data_len);
    } else if (final_data == HTTP_DATA_FINAL) {
        LOG_INF(
            "HTTP: full response (%zd bytes), status: %s",
            rsp->data_len, rsp->http_status
        );
    }

    /* Optional: print the response body */
    if (rsp->body_frag_start != NULL && rsp->body_frag_len > 0) {
        LOG_INF(
            "HTTP: body: %.*s",
            rsp->body_frag_len, rsp->body_frag_start
        );
    }

    return 0;
}

/* ========================================================================== */
/*  HTTP Thread                                                               */
/* ========================================================================== */

static int http_get_once(void)
{
    int ret;
    int sock;
    struct sockaddr_in addr;
    uint8_t recv_buf[HTTP_RECV_BUF_SIZE];
    struct http_request req = { 0 };

    /* Create TCP socket */
    sock = zsock_socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock < 0) {
        LOG_ERR("HTTP: failed to create socket (%d)", -errno);
        return -errno;
    }

    /* Fill in the server address */
    struct zsock_addrinfo hints = {
        .ai_family   = AF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP,
    };
    struct zsock_addrinfo *res = NULL;
    char port_str[6];
    snprintk(port_str, sizeof(port_str), "%d", HTTP_SERVER_PORT);

    ret = zsock_getaddrinfo(HTTP_SERVER_HOST, port_str, &hints, &res);
    if (ret != 0 || res == NULL) {
        LOG_ERR("HTTP: failed to resolve '%s' (%d)", HTTP_SERVER_HOST, ret);
        zsock_close(sock);
        return -EHOSTUNREACH;
    }

    /* Copy the resolved address */
    memcpy(&addr, res->ai_addr, sizeof(addr));
    zsock_freeaddrinfo(res);

    /* Connect */
    ret = zsock_connect(sock, (struct sockaddr *)&addr, sizeof(addr));
    if (ret < 0) {
        LOG_ERR("HTTP: connection failed (%d)", -errno);
        zsock_close(sock);
        return -errno;
    }

    /* Prepare the GET request */
    req.method = HTTP_GET;
    req.url = HTTP_PATH;
    req.host = HTTP_SERVER_HOST;
    req.protocol = "HTTP/1.1";
    req.recv_buf = recv_buf;
    req.recv_buf_len = sizeof(recv_buf);
    req.response = http_response_cb;

    /* Execute the request */
    ret = http_client_req(sock, &req, 5000, NULL);
    if (ret < 0) {
        LOG_ERR("HTTP: request failed (%d)", ret);
    } else {
        LOG_INF("HTTP: GET %s%s -> OK", HTTP_SERVER_HOST, HTTP_PATH);
    }

    zsock_close(sock);
    return ret;
}

void http_thread(void *p1, void *p2, void *p3)
{
    ARG_UNUSED(p1);
    ARG_UNUSED(p2);
    ARG_UNUSED(p3);

    /* Wait for the network to be ready */
    k_sem_take(&net_ready, K_FOREVER);
    LOG_INF("HTTP: thread started");

    while (1) {
        http_get_once();
        k_sleep(K_MSEC(HTTP_INTERVAL));
    }
}

K_THREAD_DEFINE(
    http_tid, 4096,
    http_thread, NULL, NULL, NULL,
    5, 0, 0
);

/* ========================================================================== */
/*  MQTT Thread                                                               */
/* ========================================================================== */

static void mqtt_evt_handler(struct mqtt_client *const client,
                             const struct mqtt_evt *evt)
{
    switch (evt->type) {
        case MQTT_EVT_CONNACK:
            if (evt->result != 0) {
                LOG_ERR("MQTT: connection refused (%d)", evt->result);
                return;
            }
            LOG_INF("MQTT: connected to broker");
            mqtt_connected = true;

            /* Subscribe to the topic */
            {
                struct mqtt_topic sub_topic = {
                    .topic.utf8 = MQTT_TOPIC_SUB,
                    .topic.size = strlen(MQTT_TOPIC_SUB),
                };
                struct mqtt_subscription_list sub_list = {
                    .list = &sub_topic,
                    .list_count = 1,
                    .message_id = 1,
                };
                int rc = mqtt_subscribe(client, &sub_list);
                if (rc != 0) {
                    LOG_ERR("MQTT: failed to subscribe (%d)", rc);
                } else {
                    LOG_INF("MQTT: subscribed to '%s'", MQTT_TOPIC_SUB);
                }
            }
        break;

        case MQTT_EVT_PUBLISH:
            uint32_t len = evt->param.publish.message.payload.len;
            uint8_t discard[64];

            LOG_INF(
                "MQTT: message received on '%.*s' (%u bytes)",
                evt->param.publish.message.topic.topic.size,
                evt->param.publish.message.topic.topic.utf8,
                len
            );

            uint32_t remaining = len;
            while (remaining > 0) {
                int n = mqtt_read_publish_payload(client, discard,
                                                MIN(sizeof(discard), remaining));
                if (n < 0) {
                    LOG_ERR("MQTT: failed to read payload (%d)", n);
                    break;
                }
                remaining -= n;
            }

            if (evt->param.publish.message.topic.qos == MQTT_QOS_1_AT_LEAST_ONCE) {
                mqtt_publish_qos1_ack(client, &evt->param.puback);
            }
        break;

        case MQTT_EVT_DISCONNECT:
            LOG_WRN("MQTT: disconnected from broker");
            mqtt_connected = false;
        break;

        default:
        break;
    }
}

static void build_client_id(char *buff_client_id, size_t buff_len)
{
    uint8_t id[16];
    ssize_t len = hwinfo_get_device_id(id, sizeof(id));
    size_t i = 0;

    const char *prefix = "zephyr_stress_";
    while (*prefix && i < buff_len - 1) {
        buff_client_id[i++] = *prefix++;
    }

    for (ssize_t k = 0; k < len && i < buff_len - 3; k++) {
        i += snprintk(&buff_client_id[i],
                      buff_len - i, "%02X", id[k]);
    }
    buff_client_id[i] = '\0';
}

static int mqtt_connect_broker(void)
{
    int ret;
    struct zsock_addrinfo hints = {
        .ai_family   = AF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP,
    };
    struct zsock_addrinfo *res = NULL;
    char port_str[6];

    /* Resolve broker hostname via DNS */
    snprintk(port_str, sizeof(port_str), "%d", MQTT_BROKER_PORT);

    ret = zsock_getaddrinfo(MQTT_BROKER_HOST, port_str, &hints, &res);
    if (ret != 0 || res == NULL) {
        LOG_ERR("MQTT: failed to resolve '%s' (%d)", MQTT_BROKER_HOST, ret);
        return -EHOSTUNREACH;
    }

    /* Copy the resolved address into the global sockaddr_storage */
    memset(&mqtt_broker, 0, sizeof(mqtt_broker));
    memcpy(&mqtt_broker, res->ai_addr, res->ai_addrlen);
    zsock_freeaddrinfo(res);

    build_client_id(mqtt_client_id, sizeof(mqtt_client_id));

    /* Initialize the MQTT client */
    mqtt_client_init(&mqtt_client);
    mqtt_client.evt_cb         = mqtt_evt_handler;
    mqtt_client.client_id.utf8 = mqtt_client_id;
    mqtt_client.client_id.size = strlen(mqtt_client_id);
    mqtt_client.rx_buf         = mqtt_rx_buffer;
    mqtt_client.rx_buf_size    = sizeof(mqtt_rx_buffer);
    mqtt_client.tx_buf         = mqtt_tx_buffer;
    mqtt_client.tx_buf_size    = sizeof(mqtt_tx_buffer);
    mqtt_client.transport.type = MQTT_TRANSPORT_NON_SECURE;
    mqtt_client.broker         = (struct sockaddr *)&mqtt_broker;

    /* Connect */
    ret = mqtt_connect(&mqtt_client);
    if (ret != 0) {
        LOG_ERR("MQTT: connection failed (%d)", ret);
        return ret;
    }

    LOG_INF("MQTT: resolved '%s' -> connecting...", MQTT_BROKER_HOST);
    return 0;
}

void mqtt_thread(void *p1, void *p2, void *p3)
{
    ARG_UNUSED(p1);
    ARG_UNUSED(p2);
    ARG_UNUSED(p3);

    /* Wait for the network to be ready */
    k_sem_take(&net_ready, K_FOREVER);
    LOG_INF("MQTT: thread started");

    /* MQTT main loop */
    while (1) {
        if (!mqtt_connected) {
            if (mqtt_connect_broker() != 0) {
                LOG_WRN("MQTT: retrying in 5s...");
                k_sleep(K_SECONDS(5));
                continue;
            }
        }

        struct pollfd fds[1] = {
            [0] = {
                .fd = mqtt_client.transport.tcp.sock,
                .events = ZSOCK_POLLIN 
            }
        };
        int poll_ret = zsock_poll(fds, 1, MQTT_SUB_INTERVAL);
        if (poll_ret < 0) {
            LOG_ERR("MQTT: poll error (%d)", -errno);
        } else if (poll_ret > 0 && (fds[0].revents & ZSOCK_POLLIN)) {
            int irc = mqtt_input(&mqtt_client);
            if (irc != 0 && irc != -EAGAIN) {
                LOG_ERR("MQTT: mqtt_input error (%d)", irc);
            }
        }

        /* Publish a message periodically */
        if (mqtt_connected) {
            char payload[64];
            snprintk(
                payload, sizeof(payload),
                "stress-%u", sys_rand32_get() % 10000
            );

            struct mqtt_publish_param pub = {
                .message.topic = {
                    .topic.utf8 = MQTT_TOPIC_PUB,
                    .topic.size = strlen(MQTT_TOPIC_PUB),
                },
                .message.payload.data = (uint8_t *)payload,
                .message.payload.len = strlen(payload),
                .message.topic.qos = MQTT_QOS_1_AT_LEAST_ONCE,
                .message_id = sys_rand32_get() % 65535,
                .retain_flag = 0,
            };

            int rc = mqtt_publish(&mqtt_client, &pub);
            if (rc == 0) {
                LOG_INF("MQTT: published '%s' to '%s'",
                        payload, MQTT_TOPIC_PUB);
            } else {
                LOG_ERR("MQTT: publish failed (%d)", rc);
            }
        }

        k_sleep(K_MSEC(MQTT_PUB_INTERVAL));
    }
}

K_THREAD_DEFINE(
    mqtt_tid, 4096,
    mqtt_thread, NULL, NULL, NULL,
    5, 0, 0
);

/* ========================================================================== */
/*  Main Thread                                                               */
/* ========================================================================== */

int main(void)
{
    struct net_if *iface;

    LOG_INF("=== Zephyr Ethernet Stress Test ===");

    /* Get the default network interface */
    iface = net_if_get_default();
    if (iface == NULL) {
        LOG_ERR("No network interface found");
        return -ENODEV;
    }

    /* Register the network event callback */
    net_mgmt_init_event_callback(
        &net_mgmt_cb,
        net_event_handler,
        NET_EVENT_IPV4_ADDR_ADD |
        NET_EVENT_IPV4_ADDR_DEL
    );
    net_mgmt_add_event_callback(&net_mgmt_cb);

    net_mgmt_init_event_callback(
        &ethernet_mgmt_cb,
        ethernet_event_handler,
        NET_EVENT_ETHERNET_CARRIER_ON |
        NET_EVENT_ETHERNET_CARRIER_OFF
    );
    net_mgmt_add_event_callback(&ethernet_mgmt_cb);

    /* Start the DHCP client */
    LOG_INF("Starting DHCP...");
    net_dhcpv4_start(iface);

    LOG_INF("Waiting for an IP address...");
    /* Threads were already created and are waiting on the semaphore */

    /* Keep main alive */
    while (1) {
        k_sleep(K_SECONDS(1000));
        LOG_INF("--- Heartbeat ---");
    }

    return 0;
}