from collections import deque
from datetime import datetime, timedelta
import fileinput
import logging
import logging.handlers
import os

import graypy

from RabbitQueue import RabbitSettings, RabbitQueue


def publishStates(states):
    count = len(states)
    try:
        startTime = datetime.now()

        with RabbitQueue(rabbitSettings) as queue:
            while len(states):
                queue.publish(states.pop())

        elapsedTime = datetime.now() - startTime
        logger.info("Published {0} messages in {1} seconds.".format(count, elapsedTime.total_seconds()))

    except Exception:
        logger.exception('Error on publishing {0} messages.'.format(count))


def handleInput():
    try:
        states = deque([])
        publishTime = datetime.now()

        for line in fileinput.input():
            states.append(line)

            if len(states) % 5 == 0:
                logger.info('{0} messages in local queue.'.format(len(states)))

            if len(states) >= 10 or datetime.now() - publishTime >= timedelta(minutes=1):
                publishStates(states)
                publishTime = datetime.now()

    except Exception:
        logger.exception('Global error.')


def setupLogger(seviceName, graylogHost):
    global logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    graylogHandler = graypy.GELFHandler(host=graylogHost, port=12201, facility=seviceName)
    logger.addHandler(graylogHandler)
    graylogHandler = graypy.GELFHandler(host=graylogHost, port=12201, facility=seviceName)
    logger.addHandler(graylogHandler)

    scriptDir = os.path.dirname(os.path.realpath(__file__))
    fileHandler = logging.handlers.TimedRotatingFileHandler(
        os.path.join(scriptDir, seviceName + '.log'), when='d', backupCount=3, utc=True)
    fileHandler.setFormatter(logging.Formatter('%(asctime)s %(thread)d %(levelname)s %(message)s'))
    logger.addHandler(fileHandler)


if __name__ == '__main__':
    setupLogger('PowerMTAStatesHandler', 'GrayLog host')
    logger.info('Service started.')
    global rabbitSettings
    rabbitSettings = RabbitSettings('RabbitMQ host', 5672, 'login', 'password', 'queue')
    handleInput()
    logger.info('Service stopped.')
