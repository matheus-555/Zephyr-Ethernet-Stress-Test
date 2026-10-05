# Zephyr Ethernet Stress Test

> A minimal Zephyr application to stress-test the network interface of supported boards, helping uncover bugs and stability issues in Ethernet drivers.

---

## Table of Contents

- [Overview](#overview)
- [Supported Boards](#supported-boards)
- [Requirements](#requirements)
- [Configuration](#configuration)
- [Building and Flashing](#building-and-flashing)
- [Expected Output](#expected-output)
- [How It Stresses the Interface](#how-it-stresses-the-interface)
- [Troubleshooting](#troubleshooting)
- [Directory Layout](#directory-layout)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

A minimal Zephyr application designed to **stress-test the network interface** of supported boards. Its purpose is to help uncover bugs, race conditions, and stability issues in Ethernet drivers by generating concurrent TCP (HTTP) and MQTT traffic for extended periods of time.

The application is useful for:

- ✅ Driver validation during board bring-up
- ✅ Regression testing of network drivers
- ✅ Long-run stability / soak testing
- ✅ Reproducing intermittent network-related faults

The application spawns two independent threads after the network interface obtains an IPv4 address via DHCP:

| Thread        | Responsibility                                                                 | Default Interval                  |
| ------------- | ------------------------------------------------------------------------------ | --------------------------------- |
| `http_thread` | Issues continuous HTTP `GET` requests to a configurable server                 | 500 ms                            |
| `mqtt_thread` | Maintains a persistent MQTT connection, subscribes to a topic, and publishes  | 1000 ms (publish) / 2000 ms (poll) |

Both threads share the same network stack, exercising the driver under concurrent load. Every request creates and tears down sockets, which maximizes pressure on the TCP/IP layer and, in turn, on the underlying Ethernet driver.

---

## Supported Boards

Boards specific to this project are located under the `boards/` directory. Each subdirectory may contain board-specific overlays, configuration fragments, or device tree files required for the driver under test.

```text
boards/
│  ├── <board_a>.overlay
│  ├── <board_a>.conf
│  ├── <board_b>.overlay
│  ├── <board_b>.conf
│  ├── ...
│  ├── ...
```

To build for a specific board, pass it to `west build`:

```bash
west build -b <board_name> -p always
```

Refer to the individual board folders for any pin-muxing, PHY configuration, or clock settings required by that board.

---

## Requirements

- **Zephyr RTOS** — v4.4.1
- **Zephyr SDK**  — 1.0.1
- **`west`** meta-tool
- A network with:
  - DHCP server (or static IP configuration)
  - Reachable DNS resolver
  - Reachable HTTP server
  - Reachable MQTT broker

---

## Configuration

The following options in `main.c` can be adjusted for different test scenarios:

```c
#define HTTP_SERVER_HOST   "google.com"       /* Hostname or IP */
#define HTTP_SERVER_PORT   80
#define HTTP_PATH          "/"

#define MQTT_BROKER_HOST   "broker.emqx.io"    /* Hostname or IP */
#define MQTT_BROKER_PORT   1883
#define MQTT_CLIENT_ID     "zephyr_stress_client"
#define MQTT_TOPIC_SUB     "stress/test"
#define MQTT_TOPIC_PUB     "stress/data"

#define HTTP_INTERVAL      500                 /* ms between GETs */
#define MQTT_PUB_INTERVAL  1000                /* ms between publishes */
```

---

## Building and Flashing

```bash
# Build for a supported board
west build -p always -b <board_name> .

# Flash
west flash

```

---

## Expected Output

On a healthy board, you should see a stream of log messages similar to:

```log
<inf> eth_stress: === Zephyr Ethernet Stress Test ===
<inf> eth_stress: Starting DHCP...
<inf> eth_stress: IPv4 address obtained: 192.168.1.42
<inf> eth_stress: HTTP: Thread started
<inf> eth_stress: MQTT: Thread started
<inf> eth_stress: MQTT: Resolved 'broker.emqx.io' -> connecting...
<inf> eth_stress: HTTP: GET example.com/ -> OK
<inf> eth_stress: MQTT: Connected to broker
<inf> eth_stress: MQTT: Subscribed to 'stress/test'
<inf> eth_stress: MQTT: Published 'stress-4821' to 'stress/data'
<inf> eth_stress: --- Heartbeat ---
...
```

Any deviation — timeouts, `-EINVAL`, `-ENOMEM`, `-EIO`, socket leaks, or a silent interface — is a strong indicator of a driver or stack issue worth investigating.

---

## How It Stresses the Interface

| # | Mechanism                        | Description                                                                                                     |
| - | -------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| 1 | **Concurrent TCP connections**   | HTTP and MQTT run in separate threads with independent sockets, forcing the driver to handle multiple flows.    |
| 2 | **Frequent socket lifecycle**    | The HTTP thread opens and closes a socket for every GET, hammering the driver's RX/TX descriptor rings.         |
| 3 | **Continuous MQTT traffic**      | The persistent MQTT connection generates a steady stream of small packets (PING, SUB, PUB, PUBACK).            |
| 4 | **DNS lookups**                  | Every HTTP request triggers an additional UDP flow for name resolution.                                         |
| 5 | **Long run times**               | Designed to be left running for hours or days to expose memory leaks, descriptor exhaustion, or thermal issues. |

---

## Troubleshooting

| Symptom                                      | Likely Cause                                                                                     |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `Address invalid` on HTTP or MQTT            | Using `zsock_inet_pton()` on a hostname. Use `zsock_getaddrinfo()` instead.                     |
| `HTTP: Request failed (-22)`                 | Missing `req.response` callback in `struct http_request`.                                       |
| `MQTT: Broker address invalid`               | DNS not resolved — same root cause as above.                                                    |
| No IP obtained                               | DHCP server unreachable, or driver not initialized.                                             |
| Interface goes silent after some time        | Driver bug (descriptor leak, interrupt storm) — **the class of issue this project targets**.    |
| `-ENOMEM` during DNS                         | Increase `CONFIG_HEAP_MEM_POOL_SIZE`.                                                           |

---

## Directory Layout

```text
.
├── boards/              # Board-specific overlays and configs
├── src/
│   └── main.c           # The entire application
├── CMakeLists.txt
├── prj.conf
└── README.md
```

---

## Contributing

If you find a bug in a specific board's Ethernet driver while using this stress test, please:

1. **Capture** the full serial log.
2. **Note** the exact commit hash of Zephyr used (`west list`).
3. **Describe** the hardware setup (PHY, cable, switch, link speed).
4. **Attach** the device tree used (board `.dts`/`.dtsi` + project `*.overlay`).
5. **Report** the versions:
   - Zephyr — `cat $ZEPHYR_BASE/VERSION`
   - Zephyr SDK — `cat $ZEPHYR_BASE/SDK_VERSION`
6. **Open an issue** with the log, device tree, versions, and reproduction steps.

The more boards this project covers, the better the coverage of Zephyr's Ethernet driver ecosystem becomes.