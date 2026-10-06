#!/usr/bin/env python3
"""
MQTT stress helper — publishes a JSON payload of exactly 2049 bytes
and subscribes to a topic to read incoming messages.

Usage:
    python3 mqtt_helper.py
    python3 mqtt_helper.py --host broker.emqx.io --port 1883
    python3 mqtt_helper.py --pub-topic stress/test --sub-topic stress/data
"""

import argparse
import json
import signal
import sys
import time
from datetime import datetime

import paho.mqtt.client as mqtt

# --------------------------------------------------------------------------- #
#  Configuration                                                               #
# --------------------------------------------------------------------------- #

DEFAULT_HOST       = "broker.emqx.io"
DEFAULT_PORT       = 1883
DEFAULT_PUB_TOPIC  = "stress/test"
DEFAULT_SUB_TOPIC  = "stress/data"
DEFAULT_INTERVAL   = 1.0     # seconds between publishes
DEFAULT_QOS        = 1
TARGET_JSON_SIZE   = 2049    # bytes


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #

def build_json_payload(target_size: int) -> str:
    """
    Build a JSON payload whose serialized form is exactly `target_size` bytes.

    Strategy: create a dict with a known-size filler field, then pad the
    filler until the serialized JSON reaches the target size.
    """
    ts = datetime.utcnow().isoformat() + "Z"
    base = {
        "timestamp": ts,
        "source":    "python-stress-helper",
        "seq":       0,
        "filler":    "",
    }

    # Serialize once to see how many bytes we already have
    base["seq"] = 0
    probe = json.dumps(base, separators=(",", ":"))
    missing = target_size - len(probe)
    if missing < 0:
        raise ValueError(
            f"Base JSON already has {len(probe)} bytes, "
            f"larger than the requested {target_size}"
        )

    base["filler"] = "A" * missing
    payload = json.dumps(base, separators=(",", ":"))

    # Sanity check
    assert len(payload) == target_size, (
        f"expected {target_size} bytes, got {len(payload)}"
    )
    return payload


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Connected to broker")
    else:
        print(f"[!] Connection failed, rc={rc}", file=sys.stderr)
        sys.exit(1)


def on_disconnect(client, userdata, rc, properties=None):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Disconnected (rc={rc})")


def on_message(client, userdata, msg):
    payload = msg.payload
    preview = payload[:80].decode("utf-8", errors="replace")
    suffix  = "..." if len(payload) > 80 else ""
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] "
        f"RX on '{msg.topic}' "
        f"({len(payload)} bytes, qos={msg.qos}): {preview}{suffix}"
    )


def on_publish(client, userdata, mid, rc=0, properties=None):
    # paho 2.x passes rc as 4th positional arg in some versions
    print(f"[{datetime.now().strftime('%H:%M:%S')}] TX PUBACK mid={mid}")


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] "
        f"SUBACK mid={mid}, granted_qos={granted_qos}"
    )


# --------------------------------------------------------------------------- #
#  Main                                                                        #
# --------------------------------------------------------------------------- #

def parse_args():
    p = argparse.ArgumentParser(description="MQTT stress helper")
    p.add_argument("--host",       default=DEFAULT_HOST)
    p.add_argument("--port",       type=int, default=DEFAULT_PORT)
    p.add_argument("--pub-topic",  default=DEFAULT_PUB_TOPIC)
    p.add_argument("--sub-topic",  default=DEFAULT_SUB_TOPIC)
    p.add_argument("--interval",   type=float, default=DEFAULT_INTERVAL,
                   help="seconds between publishes")
    p.add_argument("--qos",        type=int, default=DEFAULT_QOS, choices=[0, 1, 2])
    p.add_argument("--client-id",  default=None,
                   help="MQTT client id (default: auto-generated)")
    p.add_argument("--username",   default=None)
    p.add_argument("--password",   default=None)
    p.add_argument("--once",       action="store_true",
                   help="publish a single message and exit")
    return p.parse_args()


def main():
    args = parse_args()

    # paho 2.x uses CallbackAPIVersion; 1.x ignores it.
    try:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=args.client_id,
        )
    except (AttributeError, TypeError):
        client = mqtt.Client(client_id=args.client_id)

    if args.username:
        client.username_pw_set(args.username, args.password)

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message
    client.on_publish    = on_publish
    client.on_subscribe  = on_subscribe

    print(f"Connecting to {args.host}:{args.port} ...")
    client.connect(args.host, args.port, keepalive=30)
    client.loop_start()

    # Wait for connection
    time.sleep(1.0)

    # Subscribe
    client.subscribe(args.sub_topic, qos=args.qos)
    time.sleep(0.5)

    # Build the payload once; reuse it (only seq changes each iteration)
    payload = build_json_payload(TARGET_JSON_SIZE)
    print(f"Payload size: {len(payload)} bytes")

    stop = False

    def handle_sigint(sig, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_sigint)

    seq = 0
    try:
        while not stop:
            # Rebuild with an incremented seq while keeping the same size
            obj = json.loads(payload)
            obj["seq"] = seq
            # Adjust filler to keep the total size constant
            fixed = json.dumps(obj, separators=(",", ":"))
            delta = TARGET_JSON_SIZE - len(fixed)
            if delta > 0:
                obj["filler"] = obj.get("filler", "") + ("B" * delta)
            elif delta < 0:
                # Trim filler
                obj["filler"] = obj["filler"][:delta]
            payload = json.dumps(obj, separators=(",", ":"))

            info = client.publish(args.pub_topic, payload, qos=args.qos)
            if info.rc != 0:
                print(f"[!] publish failed rc={info.rc}", file=sys.stderr)
            else:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] "
                    f"TX to '{args.pub_topic}' seq={seq} "
                    f"({len(payload)} bytes, qos={args.qos})"
                )

            seq += 1

            if args.once:
                break

            time.sleep(args.interval)
    finally:
        print("\nShutting down ...")
        client.loop_stop()
        client.disconnect()
        time.sleep(0.3)


if __name__ == "__main__":
    main()