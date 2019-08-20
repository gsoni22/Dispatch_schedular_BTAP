# Databricks notebook source
from datetime import datetime
from math import ceil
import pandas as pd
import operator
import requests


# COMMAND ----------

class DispatchScheduler:
    #   TAT time with all possible combination
    tat_dict = {
        "JSG-LNJ": 1.02,
        "LNJ-JSG": 1.26,
        "JSG-Plant": 1.58,
        "LNJ-Plant": 1.50,
        "JSG-KSLK": 3.2,
        "KSLK-JSG": 4.5,
        "KSLK-Plant": 1.50,
        "JSG-Vizag": 3,
        "Vizag-KSLK": 1,
        "TXR": 2
    }
    #   Meta Data of all the BTAPs
    btap_details = {
        "VED-1": [datetime.strptime("06-02-2019",'%d-%m-%Y').date(),"L","JSG",1.5],
        "VED-2": [datetime.strptime("14-02-2019",'%d-%m-%Y').date(),"E","KSLK",2.5],
        "VED-3": [datetime.strptime("15-02-2019",'%d-%m-%Y').date(),"L","JSG",0],
        "VED-5": [datetime.strptime("04-03-2019",'%d-%m-%Y').date(),"E","KSLK",0],
        "VED-6": [datetime.strptime("23-02-2019",'%d-%m-%Y').date(),"E","LNJ",0.3],
        "VED-7": [datetime.strptime("05-02-2019",'%d-%m-%Y').date(),"E","LNJ",1.5],
        "VED-8": [datetime.strptime("09-02-2019",'%d-%m-%Y').date(),"E","LNJ",1.5],
        "VED-9": [datetime.strptime("02-02-2019",'%d-%m-%Y').date(),"E","VIZAG",0.2],
        "VED-10": [datetime.strptime("05-03-2019",'%d-%m-%Y').date(),"L","JSG",3.2],
        "VED-11": [datetime.strptime("08-02-2019",'%d-%m-%Y').date(),"E","LNJ",0.7],
        "VED-12": [datetime.strptime("14-02-2019",'%d-%m-%Y').date(),"L","JSG",1.5],
        "VED-13": [datetime.strptime("28-02-2019",'%d-%m-%Y').date(),"E","VIZAG",1.8],
        "VED-14": [datetime.strptime("25-02-2019",'%d-%m-%Y').date(),"L","JSG",3]
    }
    #   sorted_btap_txr = sorted(btap_details.items(), key=operator.itemgetter(0))
    #   Create DataFrame from the dictonery
    data = pd.DataFrame.from_dict(btap_details)

    produce_qty_lanji = 10
    produce_qty_KSPL = 5
    produce_qty_GPL = 0
    produce_qty_PDP = 0

    consume_jsg_p1 = 3.020
    consume_jsg_p2 = 4.430

    loading_jsg = 2
    loading_kspl = 1.5

    unloading_p1 = 1
    unloading_p2 = 2

    quentity_KSPL_silos_P2 = 40
    quentity_KSPL_silos_P1 = 22.5

    vessal_discharge_KSPL_port = 7.5
    stock_lanji = 20

    def __init__(self,btap,lanjis,kspls):
        self.no_of_btap = btap
        self.lanji_garh = lanjis
        self.kspl_port = kspls

    def compute_trips(self,btap_capicity):
        self.trip_from_lanj = self.lanji_garh / btap_capicity
        self.trip_from_kspl = self.kspl_port / btap_capicity


# COMMAND ----------

# Getting Location from GPS
URL = "http://40.121.165.179:7894/vedanta/lat_log/"
r = requests.get(url=URL)
btap_loc = r.json()
print(btap_loc)

# COMMAND ----------

obj1 = DispatchScheduler(btap=13,lanjis=102,kspls=60)
obj1.compute_trips(3)

# COMMAND ----------

obj1.data.head(15)

# COMMAND ----------

vessal_arrival = {
    "4 th Feb": ["VALJ","P2",22],
    "14 th Feb": ["VALJ","P2",30]
}

# COMMAND ----------

# Port Stock
kspl_J_P1 = 0
kspl_J_p2 = 28
kspl_B = 9
# // hello
# COMMAND ----------

port_supply = 62.2
vessal_arraival_dates = "2019-02-05"
port_stock = 50
lanji_stock = 120

# COMMAND ----------


# COMMAND ----------

for key,value in btap_position.items():
    if (value[1] == 'JSG'):
        print(key," :: ",value[1])

# COMMAND ----------


# COMMAND ----------

print(btap_details)

# COMMAND ----------

# Sort the BTAPs based on there TXR Date
sorted_btap_txr = sorted(btap_details.items(),key=operator.itemgetter(1))

# COMMAND ----------

kspl_txr_btap = ["VED-2","VED-3","VED-6","VED-7"]
vizag_txr_btap = ["VED-9","VED-1","VED-11","VED-8","VED-12","VED-14","VED-13","VED-5","VED-10"]

# COMMAND ----------

for element in sorted_btap_txr:
    print(element[0],":",element[1][0],element[1][1],element[1][2],element[1][3])

# COMMAND ----------


# COMMAND ----------

data = pd.DataFrame.from_dict(btap_details)

# COMMAND ----------

# API End points
# URL: http://40.121.165.179:7894/vedanta/lat_log/
# Call: GET
# POST: {    "Output:": {
#         "lati": "asdf",
#         "longi": "assd"
#     }
# }
