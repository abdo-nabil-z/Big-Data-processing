#!/usr/bin/env python3

import threading
import time
from datetime import datetime
import random
import logging

'''
the original:
1 producer  =>  1 istream  => filter  =>  1 ostream  =>  1 consumer



the following code will follow the following architecture:
	
	2 producers => 2 in streams => 2 filters => 2 filtered streams => 1 out stream => 1 consumer

	
	TemperatureInEgypt   => egyptStream   => EgyptFilter  => egyptFilteredStream   
																				  => WindowJoinOperator => joinedStream => Sink
	TemperatureInGermany => germanyStream => GermanyFilter => germanyFilteredStream 

'''

# simple stream with window size
class stream:
	def __init__(self, name, winsize):
		self._name = name
		self._winsize = winsize
		self._stream = [None]*winsize # array with winsize None elements
		self._cnt = self._rpos = self._wpos = 0
		self._mutex = threading.Condition()
	def __len__(self):
		return len(self._stream) # or self._winsize
	def __str__(self):
		return "stream(name=%s, q=%s, cnt=%d, rpos=%d, wpos=%d)" % (self._name, str(self._stream), self._cnt, self._rpos, self._wpos)
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
		self._stream[self._rpos] = None # frees the object
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
			self._mutex.wait() # wait for tuples to be dequeued
			logging.debug("put released after full stream")
		self._enqueue(t)
		self._mutex.notify()
		self._mutex.release()

	# get a something from an input stream
	def get(self):
		self._mutex.acquire()
		if self._isempty():
			logging.debug("get blocked by empty stream")
			self._mutex.wait() # wait for tuples to be enqueued
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

############################## MESSAGES

class Message:
	def __init__(self, timestamp, country, temperature):
		self.timestamp = timestamp
		self.country = country
		self.temperature = temperature
	
	def __str__(self):
		return "Message(timestamp=%f, country=%s, temperature=%d)" % (self.timestamp, self.country, self.temperature)


class JoinedTemperatureMessage:
	def __init__(self, timestamp, germany_temperature, egypt_temperature, difference, time_delta):
		self.timestamp = timestamp
		self.germany_temperature = germany_temperature
		self.egypt_temperature = egypt_temperature
		self.difference = difference
		self.time_delta = time_delta
	
	def __str__(self):
		return (
			"JoinedTemperatureMessage(timestamp=%f, germany=%d, egypt=%d, "
			"difference=%d, time_delta=%f)"
		) % (
			self.timestamp,
			self.germany_temperature,
			self.egypt_temperature,
			self.difference,
			self.time_delta
		)


class SelfJoinMessage:
    def __init__(self, timestamp, previous_temperature, current_temperature, change):
        self.timestamp = timestamp
        self.previous_temperature = previous_temperature
        self.current_temperature = current_temperature
        self.change = change

    def __str__(self):
        return (
            "SelfJoinMessage(timestamp=%f, previous=%d, current=%d, change=%d)"
        ) % (
            self.timestamp,
            self.previous_temperature,
            self.current_temperature,
            self.change
        )

#######################

# ex for tuple producer
def fountain(oStream):
	nums = range(20)
	while True:
		num = random.choice(nums)
		ts = datetime.timestamp(datetime.now());
		t = (ts, num)
		oStream.put(t)
		logging.debug("produced %d @ %f" % (num, ts))
		time.sleep(random.random()) # mimic heavy duty


# ex for tuple producer
def TemperatureInEgypt(oStream):
	nums = range(20,30)
	while True:
		num = random.choice(nums)
		ts = datetime.timestamp(datetime.now());
		message = Message(ts, "Egypt", num)
		oStream.put(message)
		logging.info("Egypt produced %s" % str(message))
		time.sleep(random.random()) # mimic heavy duty


# ex for tuple producer
def TemperatureInGermany(oStream):
	nums = range(16,24)
	while True:
		num = random.choice(nums)
		ts = datetime.timestamp(datetime.now());
		message = Message(ts, "Germany", num)
		oStream.put(message)
		logging.info("Germany produced %s" % str(message))
		time.sleep(random.random()) # mimic heavy duty




# ex for tuple consumer
def sink(iStream):
	while True:
		something = iStream.get()
		logging.debug("consumed %s" % str(something))
		time.sleep(random.random()) # mimic heavy duty


##########################   FILTERS  ######################


# ex for tuple filter
def filter(iStream, oStream):
	while True:
		(ts, num) = iStream.get()
		logging.debug("filtered %d @ %f" % (num, ts))
		oStream.put((ts, num))
		time.sleep(random.random()) # mimic heavy duty

# ex for tuple min/max filter
def filterMinMax(iStream, oStream):
	_min = float('Inf')
	_max = -float('Inf')
	while True:
		# consume one tuple
		(ts, num) = iStream.get()
		if num < _min:
			_min = num
		if num > _max:
			_max = num
		logging.debug("(min, max) = (%f, %f)" % (_min, _max))
		oStream.put((_min,_max))
		time.sleep(random.random()) # mimic heavy duty


def temperatureFilter(iStream, oStream, min_temp):
	window = []
	SIZE_FLOATING_WINDOW = 4
	while True:
		message = iStream.get()
		window.append(message.temperature)

		if message.temperature >= min_temp:
			logging.debug("%s filter passed temperature %s" % (message.country, str(message)))
			oStream.put(message)
	
		if len(window) > SIZE_FLOATING_WINDOW:
			window.pop(0)
			
		average = sum(window) / len(window)
		logging.info("%s Floating average  %s" % (message.country, str(average)))

		time.sleep(random.random())

def streamSplitter(iStream, oStream1, oStream2):
    while True:
        message = iStream.get()
        oStream1.put(message)
        oStream2.put(message)

#  FIFO  Join
def temperaturePairJoin(germanyStream, egyptStream, oStream):
	while True:
		germanyMessage = germanyStream.get()
		egyptMessage = egyptStream.get()

		difference = egyptMessage.temperature - germanyMessage.temperature
		join_timestamp = max(germanyMessage.timestamp, egyptMessage.timestamp)

		joinedMessage = (
			join_timestamp,
			germanyMessage.temperature,
			egyptMessage.temperature,
			difference
		)

		logging.debug(
			"joined Germany=%d and Egypt=%d, difference=%d" %
			(germanyMessage.temperature, egyptMessage.temperature, difference)
		)

		oStream.put(joinedMessage)



# Time window based join
def temperatureWindowJoin(germanyStream, egyptStream, oStream, window_seconds):
	while True:
		germanyMessage = germanyStream.inspect()
		egyptMessage = egyptStream.inspect()

		# If one stream is empty, wait a little bit and try again
		if germanyMessage is None or egyptMessage is None:
			time.sleep(0.05)
			continue

		time_delta = abs(germanyMessage.timestamp - egyptMessage.timestamp)

		if time_delta <= window_seconds:
			# They are close enough, so consume both
			germanyMessage = germanyStream.get()
			egyptMessage = egyptStream.get()

			difference = egyptMessage.temperature - germanyMessage.temperature
			join_timestamp = max(germanyMessage.timestamp, egyptMessage.timestamp)

			joinedMessage = JoinedTemperatureMessage(
				join_timestamp,
				germanyMessage.temperature,
				egyptMessage.temperature,
				difference,
				time_delta
			)

			logging.info("window join produced %s" % str(joinedMessage))
			oStream.put(joinedMessage)

		else:
			# They are too far apart.
			# Drop the older one because it probably will not get a match.
			if germanyMessage.timestamp < egyptMessage.timestamp:
				dropped = germanyStream.get()
				logging.debug(
					"dropped old Germany message outside window: %s" % str(dropped)
				)
			else:
				dropped = egyptStream.get()
				logging.debug(
					"dropped old Egypt message outside window: %s" % str(dropped)
				)

def temperatureSelfJoin(iStream, oStream, min_change):
    last_message = None

    while True:
        current_message = iStream.get()

        if last_message is not None:
            change = current_message.temperature - last_message.temperature

            if abs(change) >= min_change:
                joinedMessage = SelfJoinMessage(
                    current_message.timestamp,
                    last_message.temperature,
                    current_message.temperature,
                    change
                )

                logging.info("self join produced %s" % str(joinedMessage))
                oStream.put(joinedMessage)

        last_message = current_message

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

# define two streams with different sizes
#st1 = stream("Stream 1", 10)
#st2 = stream("Stream 2", 5)


##################  Streams definition  #######################
# raw streams
egyptStream = stream("Egypt Temperature Stream", 10)
germanyStream = stream("Germany Temperature Stream", 10)

# post filter streams
egyptFilteredStream = stream("Egypt Filtered Temperature Stream", 10)
germanyFilteredStream = stream("Germany Filtered Temperature Stream", 10)

# post join stream
joinedStream = stream("Joined Temperature Stream", 10)

# split streams for Germany filtered data
germanyWindowJoinStream = stream("Germany Window Join Stream", 10)
germanySelfJoinStream = stream("Germany Self Join Stream", 10)

# self join output stream
selfJoinedStream = stream("Self Joined Temperature Stream", 10)

# output streams
outStream = stream("Filtered Temperature Output Stream", 5)




# create threads for three operators one source (fountain), one filter, and one destination (sink)
#src = threading.Thread(name='fountain', target=fountain, args=(st1,))
#dst = threading.Thread(name='sink', target=sink, args=(st2,))


#####################  Threads definition  ####################
MIN_TEMP_EG = 24
MIN_TEMP_GER= 18
TIME_DELTA_BETWEEN_THE_READINGS = 1.0
MIN_CHANGE_FOR_SELF_JOIN = 2

egyptProducer = threading.Thread(name='EgyptProducer',target=TemperatureInEgypt,args=(egyptStream,),daemon=True)
germanyProducer = threading.Thread(name='GermanyProducer',target=TemperatureInGermany,args=(germanyStream,),daemon=True)

egyptFilter = threading.Thread(name='EgyptFilter', target= temperatureFilter, args=(egyptStream, egyptFilteredStream, MIN_TEMP_EG),daemon=True)
germanyFilter = threading.Thread(name='GermanyFilter', target= temperatureFilter, args=(germanyStream, germanyFilteredStream, MIN_TEMP_GER),daemon=True)

splitterThread = threading.Thread(name='GermanySplitter',target=streamSplitter,args=(germanyFilteredStream, germanyWindowJoinStream, germanySelfJoinStream),daemon=True)

#joinThread = threading.Thread(name='WindowJoin',target=temperatureWindowJoin,args=(germanyFilteredStream, egyptFilteredStream, joinedStream, TIME_DELTA_BETWEEN_THE_READINGS))
joinThread = threading.Thread(name='WindowJoin',target=temperatureWindowJoin,args=(germanyWindowJoinStream, egyptFilteredStream, joinedStream, TIME_DELTA_BETWEEN_THE_READINGS),daemon=True)

selfJoinThread = threading.Thread(name='SelfJoin',target=temperatureSelfJoin,args=(germanySelfJoinStream, selfJoinedStream, MIN_CHANGE_FOR_SELF_JOIN),daemon=True)

temperatureSink = threading.Thread(name='TemperatureSink', target=sink, args=(joinedStream,),daemon=True)

selfJoinSink = threading.Thread(name='SelfJoinSink',target=sink,args=(selfJoinedStream,),daemon=True)

temperatureSink.start()
selfJoinSink.start()

joinThread.start()
selfJoinThread.start()
splitterThread.start()

egyptFilter.start()
germanyFilter.start()

egyptProducer.start()
germanyProducer.start()




""" temperatureSink.start()
joinThread.start()
egyptFilter.start()
germanyFilter.start()
egyptProducer.start()
germanyProducer.start() """


# vim: ts=3 sw=3 sts=3 noet


try:
	while True:
		time.sleep(1)
except KeyboardInterrupt:
	print("Stopping program...")
