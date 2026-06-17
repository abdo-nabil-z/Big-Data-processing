# Real-Time Temperature Stream Processing System

## Overview

This project implements a small real-time stream processing system in Python.

The goal is to understand how modern stream processing frameworks work by building a simplified version using:

- Producers
- Streams
- Operators
- Consumers
- Threads
- Synchronization
- Join processing

The project is based on the idea of systems like Apache Flink or Spark Streaming, where continuous data is processed while it is arriving.

---

# System Architecture

The application simulates two temperature sensors:

1. Germany Temperature Sensor
2. Egypt Temperature Sensor

Both sensors continuously generate temperature data and send it into independent streams.

The streams are processed by a Join Operator which combines both inputs and calculates the temperature difference.

Final results are displayed by a Sink.

Architecture:

```
              TemperatureInGermany()
                       |
                       |
                       v

                Germany Stream

                       \
                        \
                         \
                          v

                   Temperature Join
                          |
                          |
                          v

                    Joined Stream

                          |
                          |
                          v

                        Sink


                          ^
                         /
                        /
                       /

                 Egypt Stream

                       ^
                       |

              TemperatureInEgypt()
```

---

# Components

## 1. Message Class

The Message class represents one sensor reading.

Each message contains:

- timestamp
- country
- temperature


Example:

```python
Message(
    timestamp=1781643649,
    country="Germany",
    temperature=22
)
```

Using a Message object makes the code cleaner compared to tuples.

Instead of:

```python
data[2]
```

we can write:

```python
data.temperature
```

---

# 2. Stream Class

The Stream class is the core communication mechanism.

It works as a thread-safe buffer between producers and consumers.

Responsibilities:

- Store incoming data
- Control read/write positions
- Handle full buffers
- Handle empty buffers
- Synchronize threads


Internally it uses a circular buffer:

```
+----+----+----+----+
| 20 | 21 |    |    |
+----+----+----+----+

 read        write
```

Important methods:

## put()

Used by producers.

Example:

```python
stream.put(message)
```

Adds a new message into the stream.


## get()

Used by operators and consumers.

Example:

```python
message = stream.get()
```

Reads and removes the next message.

---

# 3. Producers

## Germany Temperature Producer

Function:

```python
TemperatureInGermany()
```

Simulates a German temperature sensor.

Generated values:

```
16°C - 23°C
```

Example message:

```
Germany produced 18°C
```


## Egypt Temperature Producer

Function:

```python
TemperatureInEgypt()
```

Simulates an Egyptian temperature sensor.

Generated values:

```
20°C - 29°C
```

Example message:

```
Egypt produced 27°C
```

Both producers run independently in separate threads.

---

# 4. Join Operator

Function:

```python
temperatureJoin()
```

The join operator receives:

- Germany Stream
- Egypt Stream


Example input:

Germany:

```
18°C
```

Egypt:

```
27°C
```

Processing:

```
difference = |18 - 27|
```

Output:

```
Temperature Difference = 9°C
```

The result is written into the Joined Stream.

---

# 5. Sink

The sink is the final consumer.

Function:

```python
sink()
```

Responsibilities:

- Read processed data
- Display final output

Example:

```
Germany: 18°C
Egypt: 27°C
Difference: 9°C
```

---

# Thread Architecture

The application uses four threads:

## Thread 1

Germany producer

```
TemperatureInGermany()
```

Creates Germany temperature messages.


## Thread 2

Egypt producer

```
TemperatureInEgypt()
```

Creates Egypt temperature messages.


## Thread 3

Join operator

```
temperatureJoin()
```

Combines both streams.


## Thread 4

Sink

```
sink()
```

Displays results.

---

# Data Flow Example

Step 1:

Germany producer creates:

```
Message(
 Germany,
 18°C
)
```

Step 2:

Egypt producer creates:

```
Message(
 Egypt,
 27°C
)
```

Step 3:

Join operator combines:

```
Germany: 18
Egypt: 27
```

Step 4:

Calculates:

```
Difference = 9
```

Step 5:

Sink outputs the final result.

---

# How To Run

Clone/download the project.

Open terminal in the project directory.

Run:

```bash
python main.py
```

The program will continuously generate and process streaming data.

Stop execution:

```
CTRL + C
```

---

# Example Output

```
Germany produced Message(country=Germany, temperature=18)

Egypt produced Message(country=Egypt, temperature=27)

JOIN RESULT:
Germany=18
Egypt=27
Difference=9

Sink consumed result
```

---

# Implemented Concepts

✔ Stream processing  
✔ Producer-consumer architecture  
✔ Multi-threading  
✔ Circular buffer  
✔ Thread synchronization  
✔ Condition wait/notify  
✔ Message abstraction  
✔ Multi-stream join operator  

---

# Future Improvements

Possible extensions:

- Time-window based joins
- Minimal delta joins
- Moving average operator
- More sensor streams
- Real IoT sensor integration

---

# Conclusion

This project demonstrates the basic principles behind real-time data stream processing systems.

Multiple producers continuously generate data, streams transport the data, operators process it, and consumers output meaningful results.