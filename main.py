#!/usr/bin/env python3

import threading
import time
from datetime import datetime
import random
import logging


class Message:

    def __init__(self, timestamp, country, temperature):

        self.timestamp = timestamp
        self.country = country
        self.temperature = temperature


    def __str__(self):

        readable_time = datetime.fromtimestamp(
            self.timestamp
        ).strftime("%H:%M:%S")

        return "%s Sensor -> %d°C @ %s" % (
            self.country,
            self.temperature,
            readable_time,
        )


# simple stream with window size
class stream:
    def __init__(self, name, winsize):
        self._name = name
        self._winsize = winsize
        self._stream = [None] * winsize  # array with winsize None elements
        self._cnt = self._rpos = self._wpos = 0
        self._mutex = threading.Condition()

    def __len__(self):
        return len(self._stream)  # or self._winsize

    def __str__(self):
        return "stream(name=%s, q=%s, cnt=%d, rpos=%d, wpos=%d)" % (
            self._name,
            str(self._stream),
            self._cnt,
            self._rpos,
            self._wpos,
        )

    def _isfull(self):
        return self._rpos == self._wpos and self._cnt == self._winsize

    def _isempty(self):
        return self._rpos == self._wpos and self._cnt == 0

    def _enqueue(self, t):
        self._cnt += 1
        self._stream[self._wpos] = t
        if self._wpos + 1 == self._winsize:
            self._wpos = 0
        else:
            self._wpos += 1

    def _dequeue(self):
        t = self._stream[self._rpos]
        self._stream[self._rpos] = None  # frees the object
        if self._rpos + 1 == self._winsize:
            self._rpos = 0
        else:
            self._rpos += 1
        self._cnt -= 1
        return t

    # public methods, the stuff above should only be called withing
    # the guarded commands acquire()/release() to be thread-safe
    #
    # put a something on the outgoing stream
    def put(self, t):
        self._mutex.acquire()
        if self._isfull():
            logging.debug("stream before blocking on full = %s" % str(self))
            self._mutex.wait()  # wait for tuples to be dequeued
            logging.debug("put released after full stream")
        self._enqueue(t)
        self._mutex.notify()
        self._mutex.release()

    # get a something from an input stream
    def get(self):
        self._mutex.acquire()
        if self._isempty():
            logging.debug("get blocked by empty stream")
            self._mutex.wait()  # wait for tuples to be enqueued
            logging.debug("stream after release on empty = %s" % str(self))
        t = self._dequeue()
        self._mutex.notify()
        self._mutex.release()
        return t

    # get a something from an input stream, non blocking and non consuming
    def inspect(self):
        self._mutex.acquire()
        if self._isempty():
            logging.debug("inspect empty stream")
            t = None
        else:
            t = self._stream[self._rpos]
        self._mutex.release()
        return t


#######################


# ex for tuple producer
# Producer 1
def TemperatureInGermany(oStream):

    nums = range(16, 24)

    while True:

        temp = random.choice(nums)

        ts = datetime.timestamp(datetime.now())

        msg = Message(ts, "Germany", temp)

        oStream.put(msg)

        logging.debug("Germany produced %s" % msg)

        time.sleep(random.random())


# Producer 2
def TemperatureInEgypt(oStream):

    nums = range(20, 30)

    while True:

        temp = random.choice(nums)

        ts = datetime.timestamp(datetime.now())

        msg = Message(ts, "Egypt", temp)
        oStream.put(msg)
        logging.debug("Egypt produced %s" % msg)
        time.sleep(random.random())


# ex for tuple consumer
def sink(iStream):
    while True:
        something = iStream.get()
        logging.debug("consumed %s" % str(something))
        time.sleep(random.random())  # mimic heavy duty


def temperatureJoin(
    germanyStream,
    egyptStream,
    outputStream,
):

    WINDOW_SIZE = 0.1  # in seconds

    while True:

        germany = germanyStream.get()

        egypt = egyptStream.get()

        timeDifference = abs(germany.timestamp - egypt.timestamp)

        if timeDifference <= WINDOW_SIZE:

            tempDifference = abs(germany.temperature - egypt.temperature)

            result = (
                germany.country,
                germany.temperature,
                egypt.country,
                egypt.temperature,
                "Temp Difference",
                tempDifference,
                "Time Difference",
                timeDifference,
            )

            logging.debug("WINDOW JOIN SUCCESS %s" % str(result))

            outputStream.put(result)

        else:

            logging.debug("WINDOW JOIN FAILED. Difference=%f seconds" % timeDifference)


# ex for tuple min/max filter
def filterMinMax(iStream, oStream):
    _min = float("Inf")
    _max = -float("Inf")
    while True:
        # consume one tuple
        ts, num = iStream.get()
        if num < _min:
            _min = num
        if num > _max:
            _max = num
        logging.debug("(min, max) = (%f, %f)" % (_min, _max))
        oStream.put((_min, _max))
        time.sleep(random.random())  # mimic heavy duty


logging.basicConfig(
    level=logging.DEBUG, format="[%(levelname)s] (%(threadName)-10s) %(message)s"
)

# define two streams with different sizes
germanyStream = stream("Germany Stream", 10)


egyptStream = stream("Egypt Stream", 10)


joinedStream = stream("Joined Stream", 10)

# create threads for three operators one source (fountain), one filter, and one destination (sink)
germanyThread = threading.Thread(
    name="Germany", target=TemperatureInGermany, args=(germanyStream,)
)

egyptThread = threading.Thread(
    name="Egypt", target=TemperatureInEgypt, args=(egyptStream,)
)

joinThread = threading.Thread(
    name="Join", target=temperatureJoin, args=(germanyStream, egyptStream, joinedStream)
)

sinkThread = threading.Thread(name="Sink", target=sink, args=(joinedStream,))

joinThread.start()

sinkThread.start()

germanyThread.start()

egyptThread.start()

# vim: ts=3 sw=3 sts=3 noet
