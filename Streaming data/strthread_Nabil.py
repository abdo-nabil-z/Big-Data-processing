#!/usr/bin/env python3

import threading
import time
from datetime import datetime
import random
import logging

'''
the following code will follow the following architecture:
	
	2 producers => 2 in streams => 2 filters => 1 out stream => 1 consumer

	
TemperatureInEgypt   → egyptStream   → EgyptFilter   ┐
                                                       => outStream → TemperatureSink
TemperatureInGermany → germanyStream → GermanyFilter ┘


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


class Message:
	def __init__(self, timestamp, country, temperature):
		self.timestamp = timestamp
		self.country = country
		self.temperature = temperature
	
	def __str__(self):
		return "Message(timestamp=%f, country=%s, temperature=%d)" % (self.timestamp, self.country, self.temperature)

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
		logging.debug("Egypt produced %s" % str(message))
		time.sleep(random.random()) # mimic heavy duty


# ex for tuple producer
def TemperatureInGermany(oStream):
	nums = range(16,24)
	while True:
		num = random.choice(nums)
		ts = datetime.timestamp(datetime.now());
		message = Message(ts, "Germany", num)
		oStream.put(message)
		logging.debug("Germany produced %s" % str(message))
		time.sleep(random.random()) # mimic heavy duty




# ex for tuple consumer
def sink(iStream):
	while True:
		something = iStream.get()
		logging.debug("consumed %s" % str(something))
		time.sleep(random.random()) # mimic heavy duty

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
	while True:
		message = iStream.get()

		if message.temperature >= min_temp:
			logging.debug("%s filter passed temperature %s" % (message.country, str(message)))
			oStream.put(message)

		time.sleep(random.random())


logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

# define two streams with different sizes
#st1 = stream("Stream 1", 10)
#st2 = stream("Stream 2", 5)

# define streams
egyptStream = stream("Egypt Temperature Stream", 10)
germanyStream = stream("Germany Temperature Stream", 10)  #raw streams

# output streams
outStream = stream("Filtered Temperature Output Stream", 5)

# create threads for three operators one source (fountain), one filter, and one destination (sink)
#src = threading.Thread(name='fountain', target=fountain, args=(st1,))
#dst = threading.Thread(name='sink', target=sink, args=(st2,))

MIN_TEMP_EG = 24
MIN_TEMP_GER= 18

egyptProducer = threading.Thread(name='EgyptProducer',target=TemperatureInEgypt,args=(egyptStream,))
germanyProducer = threading.Thread(name='GermanyProducer',target=TemperatureInGermany,args=(germanyStream,))

egyptFilter = threading.Thread(name='EgyptFilter', target= temperatureFilter, args=(egyptStream, outStream, MIN_TEMP_GER))
germanyFilter = threading.Thread(name='GermanyFilter', target= temperatureFilter, args=(germanyStream, outStream, MIN_TEMP_EG))

temperatureSink = threading.Thread(name='TemperatureSink', target=sink, args=(outStream,))


temperatureSink.start()
egyptFilter.start()
germanyFilter.start()
egyptProducer.start()
germanyProducer.start()


# vim: ts=3 sw=3 sts=3 noet
