from __future__ import print_function
from pyspark.sql import Row
import xml.etree.ElementTree as ET
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext from pyspark.streaming.kafka import KafkaUtils

dict ={}

def create_dict(rt,new=None):
    global parent_tag

    for child in rt:
        if new == None :
            parent_tag = child.tag
        else :
            parent_tag = parent_tag

        if child.getchildren():
            create_dict(child,parent_tag)
        else:
#             if child.tag in dict.keys():
#                 tag = tag + child.tag

#             else:
#                 tag=child.tag
            dict[parent_tag]=child.text
    return dict

def parse_xml_to_row(xmlString):
    dct={}
    root = ET.fromstring(xmlString.encode('utf-8'))
    dct = create_dict(root)
    return Row(**dct)

def toCSVLine(data):
    return ','.join(str(d) for d in data)


#sc.stop()
# Configure Spark
conf = SparkConf().setAppName("PythonStreamingKafkaWordCount")
conf = conf.setMaster("local[*]")
sc   = SparkContext(conf=conf)
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 50)

zkQuorum, topic = 'localhost:2182', 'points-balance'
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1}) lines = kvs.map(lambda x: x[1]).map(parse_xml_to_row).map(toCSVLine)
# lines.pprint()
lines.saveAsTextFiles('file:///home/puneett/')

ssc.start()
ssc.awaitTerminationOrTimeout(100)
ssc.stop()

