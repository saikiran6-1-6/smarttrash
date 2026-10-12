# PROG6002 Assignment 2 - Smart 3-Compartment Trash Bin (edge node)
# Pico W (MicroPython) in Wokwi, stand-in for Raspberry Pi Zero.
# Extends Assignment 1: real Wi-Fi + MQTT, a real offline queue, and the servo
# now moves when a sorting command arrives from the phone pipeline.

from machine import Pin, PWM, time_pulse_us
import network
import time
import json
import socket
import struct

# ---------------- MQTT client library (built in) ----------------
# Minimal MQTT 3.1.1 client, same API as umqtt.simple (MIT licence,
# based on micropython-lib). Included here because Wokwi cannot add files.

class MQTTException(Exception):
    pass


class MQTTClient:
    def __init__(self, client_id, server, port=0, user=None, password=None,
                 keepalive=0, ssl=False, ssl_params={}):
        if port == 0:
            port = 8883 if ssl else 1883
        self.client_id = client_id
        self.sock = None
        self.server = server
        self.port = port
        self.ssl = ssl
        self.ssl_params = ssl_params
        self.pid = 0
        self.cb = None
        self.user = user
        self.pswd = password
        self.keepalive = keepalive
        self.lw_topic = None
        self.lw_msg = None
        self.lw_qos = 0
        self.lw_retain = False

    def _send_str(self, s):
        self.sock.write(struct.pack("!H", len(s)))
        self.sock.write(s)

    def _recv_len(self):
        n = 0
        sh = 0
        while 1:
            b = self.sock.read(1)[0]
            n |= (b & 0x7F) << sh
            if not b & 0x80:
                return n
            sh += 7

    def set_callback(self, f):
        self.cb = f

    def set_last_will(self, topic, msg, retain=False, qos=0):
        assert 0 <= qos <= 2
        assert topic
        self.lw_topic = topic
        self.lw_msg = msg
        self.lw_qos = qos
        self.lw_retain = retain

    def connect(self, clean_session=True):
        self.sock = socket.socket()
        addr = socket.getaddrinfo(self.server, self.port)[0][-1]
        self.sock.connect(addr)
        if self.ssl:
            try:
                import ssl as _ssl
            except ImportError:
                import ussl as _ssl
            self.sock = _ssl.wrap_socket(self.sock, **self.ssl_params)
        premsg = bytearray(b"\x10\0\0\0\0\0")
        msg = bytearray(b"\x04MQTT\x04\x02\0\0")

        sz = 10 + 2 + len(self.client_id)
        msg[6] = clean_session << 1
        if self.user is not None:
            sz += 2 + len(self.user) + 2 + len(self.pswd)
            msg[6] |= 0xC0
        if self.keepalive:
            msg[7] |= self.keepalive >> 8
            msg[8] |= self.keepalive & 0x00FF
        if self.lw_topic:
            sz += 2 + len(self.lw_topic) + 2 + len(self.lw_msg)
            msg[6] |= 0x4 | (self.lw_qos & 0x1) << 3 | (self.lw_qos & 0x2) << 3
            msg[6] |= self.lw_retain << 5

        i = 1
        while sz > 0x7F:
            premsg[i] = (sz & 0x7F) | 0x80
            sz >>= 7
            i += 1
        premsg[i] = sz

        self.sock.write(premsg, i + 2)
        self.sock.write(msg)
        self._send_str(self.client_id)
        if self.lw_topic:
            self._send_str(self.lw_topic)
            self._send_str(self.lw_msg)
        if self.user is not None:
            self._send_str(self.user)
            self._send_str(self.pswd)
        resp = self.sock.read(4)
        assert resp[0] == 0x20 and resp[1] == 0x02
        if resp[3] != 0:
            raise MQTTException(resp[3])
        return resp[2] & 1

    def disconnect(self):
        self.sock.write(b"\xe0\0")
        self.sock.close()

    def ping(self):
        self.sock.write(b"\xc0\0")

    def publish(self, topic, msg, retain=False, qos=0):
        pkt = bytearray(b"\x30\0\0\0")
        pkt[0] |= qos << 1 | retain
        sz = 2 + len(topic) + len(msg)
        if qos > 0:
            sz += 2
        assert sz < 2097152
        i = 1
        while sz > 0x7F:
            pkt[i] = (sz & 0x7F) | 0x80
            sz >>= 7
            i += 1
        pkt[i] = sz
        self.sock.write(pkt, i + 1)
        self._send_str(topic)
        if qos > 0:
            self.pid += 1
            pid = self.pid
            struct.pack_into("!H", pkt, 0, pid)
            self.sock.write(pkt, 2)
        self.sock.write(msg)
        if qos == 1:
            while 1:
                op = self.wait_msg()
                if op == 0x40:
                    sz = self.sock.read(1)
                    assert sz == b"\x02"
                    rcv_pid = self.sock.read(2)
                    rcv_pid = rcv_pid[0] << 8 | rcv_pid[1]
                    if pid == rcv_pid:
                        return
        elif qos == 2:
            assert 0

    def subscribe(self, topic, qos=0):
        assert self.cb is not None, "Subscribe callback is not set"
        pkt = bytearray(b"\x82\0\0\0")
        self.pid += 1
        struct.pack_into("!BH", pkt, 1, 2 + 2 + len(topic) + 1, self.pid)
        self.sock.write(pkt)
        self._send_str(topic)
        self.sock.write(qos.to_bytes(1, "little"))
        while 1:
            op = self.wait_msg()
            if op == 0x90:
                resp = self.sock.read(4)
                assert resp[1] == pkt[2] and resp[2] == pkt[3]
                if resp[3] == 0x80:
                    raise MQTTException(resp[3])
                return

    # Wait for a single incoming MQTT message and process it.
    # Subscribed messages are delivered to the callback.
    def wait_msg(self):
        res = self.sock.read(1)
        self.sock.setblocking(True)
        if res is None:
            return None
        if res == b"":
            raise OSError(-1)
        if res == b"\xd0":  # PINGRESP
            sz = self.sock.read(1)[0]
            assert sz == 0
            return None
        op = res[0]
        if op & 0xF0 != 0x30:
            return op
        sz = self._recv_len()
        topic_len = self.sock.read(2)
        topic_len = (topic_len[0] << 8) | topic_len[1]
        topic = self.sock.read(topic_len)
        sz -= topic_len + 2
        if op & 6:
            pid = self.sock.read(2)
            pid = pid[0] << 8 | pid[1]
            sz -= 2
        msg = self.sock.read(sz)
        self.cb(topic, msg)
        if op & 6 == 2:
            pkt = bytearray(b"\x40\x02\0\0")
            struct.pack_into("!H", pkt, 2, pid)
            self.sock.write(pkt)
        elif op & 6 == 4:
            assert 0
        return op

    # Check for pending messages without blocking.
    def check_msg(self):
        self.sock.setblocking(False)
        return self.wait_msg()


# ---------------- configuration ----------------
WIFI_SSID = "Wokwi-GUEST"        # Wokwi's simulated Wi-Fi (no password)
WIFI_PASS = ""
BROKER = "broker.hivemq.com"     # public broker for development
PORT = 1883
TEAM = "teamscu001"                   # CHANGE to a unique team name
BIN_ID = "bin01"

T_TELE = ("%s/%s/telemetry" % (TEAM, BIN_ID)).encode()
T_CMD = ("%s/%s/command" % (TEAM, BIN_ID)).encode()
T_ACK = ("%s/%s/ack" % (TEAM, BIN_ID)).encode()

# Pin mapping - same as Assignment 1 Wokwi diagram
SENSORS = {
    "recycled": {"trig": 5,  "echo": 6},
    "green":    {"trig": 13, "echo": 19},
    "landfill": {"trig": 26, "echo": 21},
}
SERVO_PIN = 12
BIN_HEIGHT_CM = 30.0
WINDOW_SIZE = 5

SAMPLE_MS = 200          # read all sensors this often
PUBLISH_MS = 2000        # telemetry period
RETRY_MS = 5000          # reconnect attempt period
FLAP_OPEN_MS = 3000      # how long the flap stays open after a command
MAX_QUEUE = 50           # offline queue limit (drops oldest)


# ---------------- ultrasonic sensor (from Assignment 1) ----------------
class Ultrasonic:
    def __init__(self, trig_pin, echo_pin):
        self.trig = Pin(trig_pin, Pin.OUT)
        self.echo = Pin(echo_pin, Pin.IN)
        self.readings = []

    def distance_cm(self):
        self.trig.low()
        time.sleep_us(2)
        self.trig.high()
        time.sleep_us(10)
        self.trig.low()
        try:
            pulse = time_pulse_us(self.echo, 1, 30000)
            if pulse < 0:
                return None
            return (pulse * 0.0343) / 2
        except:
            return None

    def smoothed_distance(self):
        d = self.distance_cm()
        if d is not None and 2 < d < 400:
            self.readings.append(d)
            if len(self.readings) > WINDOW_SIZE:
                self.readings.pop(0)
        if len(self.readings) == 0:
            return BIN_HEIGHT_CM
        return sum(self.readings) / len(self.readings)


sensors = {name: Ultrasonic(c["trig"], c["echo"]) for name, c in SENSORS.items()}


def get_fill_percent(distance_cm):
    if distance_cm is None:
        return None
    fill = ((BIN_HEIGHT_CM - distance_cm) / BIN_HEIGHT_CM) * 100
    return max(0.0, min(100.0, round(fill, 1)))


# ---------------- servo (from Assignment 1) ----------------
servo = PWM(Pin(SERVO_PIN))
servo.freq(50)


def set_servo_angle(angle):
    duty = int(1000 + (angle / 180) * 8000)
    servo.duty_u16(duty)


# Flap angle per compartment. "idle" is the parked position between sorts.
# It was 90 in Assignment 1, which is the same as "green", so the flap would not
# visibly move for green. Parked at 0 here; change it to suit your mechanism.
SERVO_POSITIONS = {"recycled": 30, "green": 90, "landfill": 150}
IDLE_ANGLE = 0


# ---------------- Wi-Fi + MQTT with offline queue ----------------
wlan = network.WLAN(network.STA_IF)
wlan.active(True)

client = None
mqtt_connected = False
offline_queue = []
close_at = None


def queue_message(msg):
    offline_queue.append(msg)
    if len(offline_queue) > MAX_QUEUE:
        offline_queue.pop(0)
    print("OFFLINE - queued. Depth:", len(offline_queue))


def flush_queue():
    global mqtt_connected
    while offline_queue and mqtt_connected:
        try:
            client.publish(T_TELE, offline_queue[0])
            offline_queue.pop(0)
        except Exception:
            mqtt_connected = False


def publish_telemetry(payload):
    global mqtt_connected
    msg = json.dumps(payload)
    if mqtt_connected:
        try:
            client.publish(T_TELE, msg)
            print("MQTT PUBLISH ->", msg)
            return
        except Exception:
            mqtt_connected = False
    queue_message(msg)


def on_message(topic, msg):
    """Runs when a sorting command arrives: {"id": 7, "class": "green"}"""
    global close_at
    try:
        d = json.loads(msg)
        cls = d["class"]
        angle = SERVO_POSITIONS[cls]
    except Exception:
        print("bad command:", msg)
        return
    set_servo_angle(angle)
    close_at = time.ticks_add(time.ticks_ms(), FLAP_OPEN_MS)
    print("Servo ->", cls, "angle:", angle)
    try:
        client.publish(T_ACK, json.dumps({"id": d.get("id", 0)}))
    except Exception:
        pass


def try_connect():
    """One reconnect attempt. Returns True when MQTT is up."""
    global client, mqtt_connected
    if not wlan.isconnected():
        try:
            wlan.connect(WIFI_SSID, WIFI_PASS)
        except Exception:
            pass
        return False
    try:
        client = MQTTClient("pico-" + TEAM + "-" + BIN_ID, BROKER, port=PORT, keepalive=30)
        client.set_callback(on_message)
        client.connect()
        client.subscribe(T_CMD)
        mqtt_connected = True
        print("MQTT connected")
        flush_queue()
        return True
    except Exception as e:
        print("MQTT connect failed:", e)
        mqtt_connected = False
        return False


# ---------------- main loop ----------------
print("Smart Bin started")
set_servo_angle(IDLE_ANGLE)

capacities = {}
last_read = last_pub = last_try = time.ticks_ms()
last_read = time.ticks_add(last_read, -SAMPLE_MS)      # sample straight away
last_try = time.ticks_add(last_try, -RETRY_MS)         # try to connect straight away

while True:
    now = time.ticks_ms()

    # auto-reconnect
    if not mqtt_connected and time.ticks_diff(now, last_try) >= RETRY_MS:
        last_try = now
        try_connect()

    # receive commands (non-blocking)
    if mqtt_connected:
        try:
            client.check_msg()
        except Exception:
            mqtt_connected = False

    # read sensors
    if time.ticks_diff(now, last_read) >= SAMPLE_MS:
        last_read = now
        for name, sensor in sensors.items():
            dist = sensor.smoothed_distance()
            capacities[name] = {
                "fill_percent": get_fill_percent(dist),
                "raw_distance_cm": round(dist, 1) if dist is not None else None,
                "status": "ok" if dist is not None else "sensor_fault",
            }

    # close the flap after the timeout
    if close_at is not None and time.ticks_diff(now, close_at) >= 0:
        set_servo_angle(IDLE_ANGLE)
        close_at = None

    # publish telemetry
    if capacities and time.ticks_diff(now, last_pub) >= PUBLISH_MS:
        last_pub = now
        payload = {
            "schema_version": "1.0",
            "bin_id": "BIN-001",
            "timestamp": now // 1000,          # uptime in seconds
            "firmware_version": "2.0.0-wokwi",
            "capacities": capacities,
            "system_health": {
                "mqtt_connected": mqtt_connected,
                "wifi_connected": wlan.isconnected(),
                "offline_queue_depth": len(offline_queue),
            },
            "alerts": [],
        }
        for name, data in capacities.items():
            if data["fill_percent"] is not None and data["fill_percent"] > 85:
                payload["alerts"].append(name + "_above_threshold")
        publish_telemetry(payload)

    time.sleep_ms(20)