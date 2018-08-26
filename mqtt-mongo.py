#!/usr/bin/python
# -*- coding: utf-8 -*-
"""mqtt-mongo - Store MQTT messages to Mongo DB.

Subscribes to a MQTT server and stores incoming messages to Mongo DB.

Usage:
  mqtt-mongo.py [-v] [--mqtt-host=host] [--mqtt-port=port]
  mqtt-mongo.py -h | --help
  mqtt-mongo.py --version

Arguments:
  None.

Options:
  --mqtt-host=host  Hostname of the MQTT-server [default: localhost]
  --mqtt-port=port  Port of the MQTT-server (broker) [default: 1883]
  -v --verbose      More output.
  -h --help         Show this screen.
  --version         Show version.
"""
import logging
import os
import signal
import sys
from json import loads
from time import ctime

import paho.mqtt.client as mqtt
from docopt import docopt
from pymongo import MongoClient

__author__ = "Alexander Streicher"
__email__ = "ixtalo@gmail.com"
__copyright__ = "Copyright (C) 2018 Alexander Streicher"
__license__ = "GPL"
__version__ = "1.2"
__date__ = "2018-08-25"
__updated__ = '2018-08-26'
__status__ = "Production"

MQTT_SUBSCRIBE_TOPICS = ('#',)  # list of MQTT subscription topics
MONGO_DB = 'mqtt'  # database name

######################################################
######################################################
######################################################

MYNAME = 'mqtt-mongo'

# DEBUG = os.environ.get('MYLOGGER_DEBUG', 0)
DEBUG = 0
TESTRUN = 0
PROFILE = 0

EXITCODE_OK = 0
EXITCODE_MQTT_CON = 1
EXITCODE_MQTT_EXCEPTION = 2
EXITCODE_MQTT_DISCONNECT = 3

## global fields/variables
logger = None
mqtt_client = None
db_messages = None


## CTRL+C handling
## http://stackoverflow.com/questions/1112343/how-do-i-capture-sigint-in-python
def signal_handler_sigint(signal, frame):
    logger.warn('Ctrl+C pressed or SIGINT signal received. Stopping!')
    print('Ctrl+C pressed or SIGINT signal received. Stopping!')
    cleanup()
    sys.exit(EXITCODE_OK)


# def signal_handler_sigusr1(signal, frame):
#    logger.warn('SIGUSR1 sent! Persisting...')

def cleanup():
    ## MQTT disconnect
    errno = mqtt_client.disconnect()
    if errno != mqtt.MQTT_ERR_SUCCESS:
        logger.warn("Problem disconnecting from MQTT server: %s", mqtt.error_string(errno))
        sys.exit(EXITCODE_MQTT_DISCONNECT)


def on_connect(client, userdata, rc):
    logger.info("MQTT: Connected with result code %d", rc)
    if rc == mqtt.MQTT_ERR_SUCCESS:
        ## Subscribing here in on_connect() means that if we lose the
        ## connection and reconnect then subscriptions will be renewed.
        subscribe(client)
    else:  # not mqtt.MQTT_ERR_SUCCESS
        logger.warn("MQTT: Connection error: %s", mqtt.error_string(rc))


def on_disconnect(client, userdata, rc):
    if rc == mqtt.MQTT_ERR_SUCCESS:
        logger.debug('MQTT: disconnect successful.')
    else:
        logger.warn("MQTT: disconnected! %s (%d)", mqtt.error_string(rc), rc)
        if rc == mqtt.MQTT_ERR_NOMEM:
            logger.warn('MQTT: MQTT_ERR_NOMEM - is another instance running?!')

            try:
                errno = client.reconnect()
                if errno == mqtt.MQTT_ERR_SUCCESS:
                    logger.warn('Reconnect after disconnect OK.')
                else:
                    logger.error("Problem reconnecting to MQTT server: %s", mqtt.error_string(errno))
            except Exception as ex:
                logger.error("Exception when reconnecting to MQTT server: %s", ex)


def on_message(client, userdata, msg):
    payload = msg.payload

    ## convert binary payload
    try:
        payload = payload.decode('utf8')
    except Exception as ex:
        logger.warning("Payload decoding problem: %s", ex)

    ## try to convert to JSON
    try:
        payload = loads(payload, parse_float=True)
    except Exception as ex:
        ## ignore exception
        logger.debug("(ignoreable?) payload JSON parsing problem: %s", ex)

    ## construct container
    dbdata = {
        'timestamp': msg.timestamp,
        'topic': msg.topic,
        'payload': payload
    }

    ## mid seems to be 0 most of the time... only add it if not 0
    if msg.mid != 0:
        dbdata['mid'] = msg.mid

    ## store in database
    try:
        dbid = db_messages.insert_one(dbdata).inserted_id
        logger.info("MongoDB inserted, id=%s", dbid)
    except Exception as ex:
        logger.error("MongoDB insert error: %s", ex)


def on_subscribe(client, userdata, mid, granted_qos):
    logger.info("MQTT: subscribed. (granted QOS=%s)", str(granted_qos))


def on_unsubscribe(client, userdata, mid):
    logger.warning('MQTT: Unsubscribed!')
    subscribe(client)


def on_log(client, userdata, level, buf):
    logger.debug("%s: %s", level, buf)


def subscribe(client):
    for topic in MQTT_SUBSCRIBE_TOPICS:
        res, mid = client.subscribe(topic)
        if res == mqtt.MQTT_ERR_SUCCESS:
            logger.debug("MQTT: subscribed to '%s'", topic)
        else:
            logger.warning("MQTT: Could not subscribe to topic '%s'. Result:%s", mqtt.error_string(res))


def _setup_logging(verbose=False):
    ## set up logging
    global logger
    logger = logging.getLogger(MYNAME)
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(
        fmt=logging.Formatter('%(asctime)s - %(levelname)-8s - %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
    )
    logger.addHandler(console_handler)  # without handler setLevel is not working!
    logger.setLevel(logging.INFO)  # default log level
    if verbose:
        logger.setLevel(logging.DEBUG)
    if DEBUG:
        ## DEBUG overrides
        logger.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)


def _log_startup_info():
    ## store the current log level and switch level
    actual_loglevel = logger.getEffectiveLevel()
    logger.setLevel(logging.INFO)
    logger.info("NEW RUN, version:%s (%s), log-level:%s, cwd:%s, euid:%d, egid:%d, pid:%d",
                __version__,
                __updated__,
                logging.getLevelName(actual_loglevel),
                os.getcwd(),
                os.geteuid(),
                os.getegid(),
                os.getpid()
                )
    ## restore log level
    logger.setLevel(actual_loglevel)


def _setup_mqtt(mqtt_host, mqtt_port):
    ## set up MQTT
    logger.debug("Initializing MQTT connection...")
    global mqtt_client
    mqtt_client = mqtt.Client(client_id="%s(%s)" % (MYNAME, __version__), clean_session=False)
    ###mqtt_client.subscribe(MQTT_SUBSCRIBE_TOPIC)  # done in on_connect
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_subscribe = on_subscribe
    mqtt_client.on_unsubscribe = on_unsubscribe
    mqtt_client.on_message = on_message
    if DEBUG:
        ## in DEBUG mode attach our logging facility
        mqtt_client.on_log = on_log
    try:
        errno = mqtt_client.connect(mqtt_host, mqtt_port)
        if errno == mqtt.MQTT_ERR_SUCCESS:
            logger.info("MQTT: connected to %s:%d as '%s'", mqtt_host, mqtt_port, mqtt_client._client_id)
        else:
            logger.error("Problem connecting to MQTT server: %s", mqtt.error_string(errno))
            return EXITCODE_MQTT_CON
    except Exception as ex:
        logger.error("Exception when connecting to MQTT server %s: %s", mqtt_host, ex)
        return EXITCODE_MQTT_EXCEPTION
    return 0


def _setup_database_connection():
    global db_messages
    mongo = MongoClient()
    db_messages = mongo[MONGO_DB].messages  # <MONGO_DB>.messages  (messages collection)


def _setup_signalling():
    ## https://en.wikipedia.org/wiki/Unix_signal
    signal.signal(signal.SIGINT, signal_handler_sigint)
    # signal.signal(signal.SIGUSR1, signal_handler_sigusr1)
    print('Start time: %s' % ctime())
    print("Press Ctrl+C to quit (PID:%d)" % os.getpid())


def main():
    arguments = docopt(__doc__, version="mqtt-mongo v%s" % __version__)
    verbose = arguments['--verbose']
    mqtt_host = arguments['--mqtt-host']
    mqtt_port = int(arguments['--mqtt-port'])

    ## logging
    _setup_logging(verbose)
    logger.debug("Command line arguments: %s", arguments)

    ## MQTT connection
    result = _setup_mqtt(mqtt_host, mqtt_port)
    if result != 0:
        return result  # this calls sys.exit(result)

    ## Database
    _setup_database_connection()

    ## CTRL+C handling
    _setup_signalling()

    ## main loop
    # Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a manual interface.
    mqtt_client.loop_forever(retry_first_connection=False)


if __name__ == "__main__":
    if DEBUG:
        print("---------------- DEBUG MODE -----------------")
        if "--verbose" not in sys.argv: sys.argv.append("--verbose")
        # if "--dry-run" not in sys.argv: sys.argv.append("--dry-run")
    if TESTRUN:
        print("---------------- TEST RUN -----------------")
        import doctest
        doctest.testmod()
    if PROFILE:
        print("---------------- PROFILING -----------------")
        import cProfile
        import pstats
        profile_filename = 'pydiskdir_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())
