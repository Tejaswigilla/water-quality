import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
#from commons.Utils import Utils


def splitComma(line: str):
    splits = line.split(",")
    return "{}, {}".format(splits [0],splits[6])

if __name__ == "__main__":
	    conf = SparkConf().setAppName("water").setMaster("local[*]")
	    sc = SparkContext(conf = conf)
	    
	    water = sc.textFile("water_quality.csv")

	    water1 = water.filter(lambda line: (line.split(",")[6]) =="Fluoride")
	    
	    t = water1.map(splitComma)
	  
	    t.saveAsTextFile("out/water_sol3.csv")
