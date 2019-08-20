import copy
import json
import math
import operator
import sqlalchemy
import sys
import urllib
from calendar import monthrange
from datetime import datetime
from datetime import timedelta

import pandas as pd
from django.core.serializers.json import DjangoJSONEncoder
from read_json_file import read_input_json

obj1 = None
getGlobalList = None


class DispatchScheduler:
    #   TAT time with all possible combination

    def __init__(self,plant1_demand=0,plant2_demand=0,count_kslk_supply_jsg=0,count_kslk_supply_balco=0,
                 count_lanj_supply_jsg=0,count_lanj_supply_balco=0,sortedDict=[],flag2=0,flag=None,
                 timeSpend=0,resultTable=[],count_jsg_to_lnj=0,
                 ls_jsg_to_lnj_btap=[],ls_balco_to_lnj_btap=[],manual_delay_sortedDict=[],ls2=[],plant1_arrival=[],
                 plant2_arrival=[],jsg_to_kslk_ls=[],balco_to_kslk_ls=[],
                 jsg_to_lnj_list=[],balco_to_lnj_list=[],count_jsgDep_not_lnjArr=0,count_balcoDep_not_lnjArr=0,
                 holdlist=[],plantdates=[],initial_cylon_capacity=20,
                 lnj_jsg=None,lnj_balco=None,kslk_jsg=None,kslk_balco=None,month=None,year=None,no_of_btap=None,
                 btap_txr_details=dict(),tat_dict=dict(),vessel_arrival_date=[],btap_capacity=0):
        # self.demand_jsg = lnj_jsg + kslk_jsg
        # self.demand_balco = lnj_balco + kslk_balco
        self.demand_jsg = 0
        self.demand_balco = 0
        self.btap_mapping = {}
        self.rev_btap_mapping = {}
        self.plant1_demand = math.ceil(self.demand_jsg / 3) // 3000
        self.plant2_demand = (self.demand_jsg - math.ceil(self.demand_jsg / 3)) // 3000
        self.count_kslk_supply_jsg = count_kslk_supply_jsg
        self.count_kslk_supply_balco = count_kslk_supply_balco
        self.count_lanj_supply_jsg = count_lanj_supply_jsg
        self.count_lanj_supply_balco = count_lanj_supply_balco
        self.sortedDict = sortedDict
        self.flag2 = flag2
        self.no_of_btap = no_of_btap
        self.flag = [0] * no_of_btap
        print("no_of_btap",no_of_btap)
        self.initial_cylon_capacity = initial_cylon_capacity
        self.cylon_capacity = [initial_cylon_capacity] * 30
        self.initial_cylon_capacity = initial_cylon_capacity
        self.sys_date = datetime.strptime("01-06-2019",'%d-%m-%Y')
        self.vessel_arrival_date = vessel_arrival_date
        self.timeSpend = timeSpend
        self.resultTable = resultTable
        self.count_jsg_to_lnj = count_jsg_to_lnj
        self.ls_jsg_to_lnj_btap = ls_jsg_to_lnj_btap
        self.ls_balco_to_lnj_btap = ls_balco_to_lnj_btap
        self.manual_delay_sortedDict = manual_delay_sortedDict
        self.sys_date = self.sys_date
        self.ls2 = ls2
        self.plant1_arrival = plant1_arrival
        self.plant2_arrival = plant2_arrival
        self.jsg_to_kslk_ls = jsg_to_kslk_ls
        self.balco_to_kslk_ls = balco_to_kslk_ls
        self.jsg_to_lnj_list = jsg_to_lnj_list
        self.balco_to_lnj_list = balco_to_lnj_list
        self.count_jsgDep_not_lnjArr = count_jsgDep_not_lnjArr
        self.count_balcoDep_not_lnjArr = count_balcoDep_not_lnjArr
        self.holdlist = holdlist
        self.plantdates = plantdates
        self.lnj_jsg = lnj_jsg
        self.lnj_balco = lnj_balco
        self.kslk_jsg = kslk_jsg
        self.kslk_balco = kslk_balco
        self.month = month
        self.countB = 0
        self.countL = 0
        self.countK = 0
        self.btap_info = None
        self.btap_txr_details = btap_txr_details
        self.tat_dict = tat_dict
        self.initial_cylon_capacity = 0
        self.year = year
        self.btap_capacity = btap_capacity

    # self.sys_date = sys_date

    def compute_trips_and_btap(self,btap_capicity):
        global obj1,getGlobalList

        self.lnj_to_jsg_trip = self.lnj_jsg // btap_capicity
        self.lnj_to_balco_trip = self.lnj_balco // btap_capicity

        self.kslk_to_jsg_trip = self.kslk_jsg // btap_capicity
        self.kslk_to_balco_trip = self.kslk_balco // btap_capicity

        self.tat_lanj_to_jsg = math.ceil(
            self.tat_dict['LNJ-Plant'] + self.tat_dict['LNJ-JSG'] + self.tat_dict['JSG-Plant'] + self.tat_dict[
                'JSG-LNJ'])
        self.tat_kslk_to_jsg = math.ceil(
            self.tat_dict['KSLK-Plant'] + self.tat_dict['KSLK-JSG'] + self.tat_dict['JSG-Plant'] + self.tat_dict[
                'JSG-KSLK'])

        self.tat_lanj_to_balco = math.ceil(
            self.tat_dict['LNJ-Plant'] + self.tat_dict['LNJ-BALCO'] + self.tat_dict['BALCO-Plant'] + self.tat_dict[
                'BALCO-LNJ'])

        self.tat_kslk_to_balco = math.ceil(
            self.tat_dict['KSLK-Plant'] + self.tat_dict['KSLK-BALCO'] + self.tat_dict['BALCO-Plant'] + self.tat_dict[
                'BALCO-KSLK'])

        self.btap_from_lanji_to_jsg = math.ceil(self.lnj_to_jsg_trip / self.tat_lanj_to_jsg)
        self.btap_from_lanji_to_balco = math.ceil(self.lnj_to_balco_trip / self.tat_lanj_to_balco)


def split_dict():
    global obj1,getGlobalList
    ls = []
    for k,v in obj1.btap_info.items():
        if v[1] == 'JSG':
            ls.append([k,v])
    for k,v in obj1.btap_info.items():
        if v[1] == 'LNJ':
            ls.append([k,v])
    for k,v in obj1.btap_info.items():
        if v[1] == 'BALCO':
            ls.append([k,v])
    for k,v in obj1.btap_info.items():
        if v[1] == 'KSLK':
            ls.append([k,v])
    for k,v in obj1.btap_info.items():
        if v[1] == 'Vizag':
            ls.append([k,v])
    for k,v in obj1.btap_info.items():
        if v[1] == 'KSLK-TXR':
            ls.append([k,v])
    for k,v in obj1.btap_info.items():
        if v[1] == 'TXR':
            ls.append([k,v])

    return ls


def check(sysDate,date):
    global obj1,getGlobalList
    return 0 if sysDate > date else -1


class ArrivalDeparture:
    # def __init__(self, btap_in_balco=None, hist_balco_arrival=None, hist_balco_dep=None, hist_jsg_arrival=None,
    #              hist_jsg_dep=None, hist_lnj_arrival=None, hist_lnj_dep=None, hist_vizag_arrival=None,
    #              hist_vizag_dep=None, hist_kslk_arrival=None, hist_kslk_dep=None, hist_balco_circuit=None,
    #              hist_jsg_circuit=None, hist_kslk_maintain=None, hist_count_lnj_supply_jsg=None,
    #              hist_count_kslk_supply_jsg=None, hist_count_lnj_supply_balco=None, hist_count_kslk_supply_balco=None,
    #              hist_flag=None, hist_txr_flag=None, hist_store_txr=None, hist_count_jsgDep_not_lnjArr=None,
    #              hist_count_balcoDep_not_lnjArr=None, hist_plant1_demand=None, hist_plant2_demand=None,
    #              hist_plant_record=None, hist_hold_records=None, balco_arrival=None, balco_dep=None, jsg_arrival=None,
    #              jsg_dep=None, lnj_arrival=None, lnj_dep=None, vizag_arrival=None, vizag_dep=None, kslk_arrival=None,
    #              kslk_dep=None, kslk_maintenance_arrival=None, btap_in_vizag=None):
    def __init__(self,btap_in_balco=[],hist_balco_arrival=[],hist_balco_dep=[],hist_jsg_arrival=[],
                 hist_jsg_dep=[],hist_lnj_arrival=[],hist_lnj_dep=[],hist_vizag_arrival=[],hist_vizag_dep=[],
                 hist_kslk_arrival=[],hist_kslk_dep=[],hist_balco_circuit=[],hist_jsg_circuit=[],
                 hist_kslk_maintain=[],hist_count_lnj_supply_jsg=[],hist_count_kslk_supply_jsg=[],
                 hist_count_lnj_supply_balco=[],hist_count_kslk_supply_balco=[],hist_flag=[],hist_txr_flag=[],
                 hist_store_txr=[],hist_count_jsgDep_not_lnjArr=[],hist_count_balcoDep_not_lnjArr=[],
                 hist_plant1_demand=[],hist_plant2_demand=[],hist_plant_record=[],hist_hold_records=[],
                 balco_arrival=[],balco_dep=[],jsg_arrival=[],jsg_dep=[],lnj_arrival=[],lnj_dep=[],
                 vizag_arrival=[],vizag_dep=[],kslk_arrival=[],kslk_dep=[],
                 kslk_maintenance_arrival=[],btap_in_vizag=[]):
        self.btap_in_balco = ["VED-15","VED-16","VED-17","VED-18","VED-19","VED-20","VED-21","VED-22","VED-23",
                              "VED-24","VED-25"]
        self.hist_balco_arrival = hist_balco_arrival
        self.hist_balco_dep = hist_balco_dep
        self.hist_jsg_arrival = hist_jsg_arrival
        self.hist_jsg_dep = hist_jsg_dep
        self.hist_lnj_arrival = hist_lnj_arrival
        self.hist_lnj_dep = hist_lnj_dep
        self.hist_vizag_arrival = hist_vizag_arrival
        self.hist_vizag_dep = hist_vizag_dep
        self.hist_kslk_arrival = hist_kslk_arrival
        self.hist_kslk_dep = hist_kslk_dep
        self.hist_balco_circuit = hist_balco_circuit
        self.hist_jsg_circuit = hist_jsg_circuit
        self.hist_kslk_maintain = hist_kslk_maintain
        self.hist_count_lnj_supply_jsg = hist_count_lnj_supply_jsg
        self.hist_count_kslk_supply_jsg = hist_count_kslk_supply_jsg
        self.hist_count_lnj_supply_balco = hist_count_lnj_supply_balco
        self.hist_count_kslk_supply_balco = hist_count_kslk_supply_balco
        self.hist_flag = hist_flag
        self.hist_txr_flag = hist_txr_flag
        self.hist_store_txr = hist_store_txr
        self.hist_count_jsgDep_not_lnjArr = hist_count_jsgDep_not_lnjArr
        self.hist_count_balcoDep_not_lnjArr = hist_count_balcoDep_not_lnjArr
        self.hist_plant1_demand = hist_plant1_demand
        self.hist_plant2_demand = hist_plant2_demand
        self.hist_plant_record = hist_plant_record
        self.hist_hold_records = hist_hold_records
        self.balco_arrival = balco_arrival
        self.balco_dep = balco_dep
        self.jsg_arrival = jsg_arrival
        self.jsg_dep = jsg_dep
        self.lnj_arrival = lnj_arrival
        self.lnj_dep = lnj_dep
        self.vizag_arrival = vizag_arrival
        self.vizag_dep = vizag_dep
        self.kslk_arrival = kslk_arrival
        self.kslk_dep = kslk_dep
        self.kslk_maintenance_arrival = kslk_maintenance_arrival
        self.btap_in_vizag = btap_in_vizag


def clearList3():
    global obj1,getGlobalList
    getGlobalList.hist_balco_arrival.extend(copy.deepcopy(getGlobalList.balco_arrival))
    getGlobalList.hist_balco_dep.extend(copy.deepcopy(getGlobalList.balco_dep))

    getGlobalList.hist_jsg_arrival.extend(copy.deepcopy(getGlobalList.jsg_arrival))
    getGlobalList.hist_jsg_dep.extend(copy.deepcopy(getGlobalList.jsg_dep))

    getGlobalList.hist_lnj_arrival.extend(copy.deepcopy(getGlobalList.lnj_arrival))
    getGlobalList.hist_lnj_dep.extend(copy.deepcopy(getGlobalList.lnj_dep))

    getGlobalList.hist_vizag_arrival.extend(copy.deepcopy(getGlobalList.vizag_arrival))
    getGlobalList.hist_vizag_dep.extend(copy.deepcopy(getGlobalList.vizag_dep))

    getGlobalList.hist_kslk_arrival.extend(copy.deepcopy(getGlobalList.kslk_arrival))
    getGlobalList.hist_kslk_dep.extend(copy.deepcopy(getGlobalList.kslk_dep))

    getGlobalList.hist_kslk_maintain.extend(copy.deepcopy(getGlobalList.kslk_maintenance_arrival))

    getGlobalList.hist_count_lnj_supply_jsg[-1] = getGlobalList.hist_count_lnj_supply_jsg[
                                                      -1] + obj1.count_lanj_supply_jsg
    getGlobalList.hist_count_kslk_supply_jsg[-1] = getGlobalList.hist_count_kslk_supply_jsg[
                                                       -1] + obj1.count_kslk_supply_jsg
    getGlobalList.hist_count_lnj_supply_balco[-1] = getGlobalList.hist_count_lnj_supply_balco[
                                                        -1] + obj1.count_lanj_supply_balco
    getGlobalList.hist_count_kslk_supply_balco[-1] = getGlobalList.hist_count_kslk_supply_balco[
                                                         -1] + obj1.count_kslk_supply_balco
    getGlobalList.hist_count_balcoDep_not_lnjArr[-1] = getGlobalList.hist_count_balcoDep_not_lnjArr[
                                                           -1] + obj1.count_balcoDep_not_lnjArr
    getGlobalList.hist_count_jsgDep_not_lnjArr[-1] = getGlobalList.hist_count_jsgDep_not_lnjArr[
                                                         -1] + obj1.count_jsgDep_not_lnjArr

    getGlobalList.hist_flag[-1].extend(copy.deepcopy(obj1.flag))
    getGlobalList.hist_plant_record[-1].extend(copy.deepcopy(obj1.plantdates))

    getGlobalList.hist_plant1_demand[-1] = getGlobalList.hist_plant1_demand[-1] + obj1.plant1_demand
    getGlobalList.hist_plant2_demand[-1] = getGlobalList.hist_plant2_demand[-1] + obj1.plant2_demand

    obj1.plant1_arrival.clear()
    obj1.plant2_arrival.clear()
    obj1.plantdates.clear()

    getGlobalList.balco_arrival.clear()
    getGlobalList.balco_dep.clear()

    getGlobalList.jsg_arrival.clear()
    getGlobalList.jsg_dep.clear()

    getGlobalList.lnj_arrival.clear()
    getGlobalList.lnj_dep.clear()

    getGlobalList.vizag_arrival.clear()
    getGlobalList.vizag_dep.clear()

    getGlobalList.kslk_arrival.clear()
    getGlobalList.kslk_dep.clear()

    getGlobalList.kslk_maintenance_arrival.clear()


def clearList2():
    global obj1,getGlobalList

    obj1.plant1_arrival.clear()
    obj1.plant2_arrival.clear()
    obj1.plantdates.clear()

    getGlobalList.balco_arrival.clear()
    getGlobalList.balco_dep.clear()

    getGlobalList.jsg_arrival.clear()
    getGlobalList.jsg_dep.clear()

    getGlobalList.lnj_arrival.clear()
    getGlobalList.lnj_dep.clear()

    getGlobalList.vizag_arrival.clear()
    getGlobalList.vizag_dep.clear()

    getGlobalList.kslk_arrival.clear()
    getGlobalList.kslk_dep.clear()

    getGlobalList.kslk_maintenance_arrival.clear()


def clearList():
    global obj1,getGlobalList
    getGlobalList.hist_balco_arrival.extend(copy.deepcopy(getGlobalList.balco_arrival))
    getGlobalList.hist_balco_dep.extend(copy.deepcopy(getGlobalList.balco_dep))

    getGlobalList.hist_jsg_arrival.extend(copy.deepcopy(getGlobalList.jsg_arrival))
    getGlobalList.hist_jsg_dep.extend(copy.deepcopy(getGlobalList.jsg_dep))

    getGlobalList.hist_lnj_arrival.extend(copy.deepcopy(getGlobalList.lnj_arrival))
    getGlobalList.hist_lnj_dep.extend(copy.deepcopy(getGlobalList.lnj_dep))

    getGlobalList.hist_vizag_arrival.extend(copy.deepcopy(getGlobalList.vizag_arrival))
    getGlobalList.hist_vizag_dep.extend(copy.deepcopy(getGlobalList.vizag_dep))

    getGlobalList.hist_kslk_arrival.extend(copy.deepcopy(getGlobalList.kslk_arrival))
    getGlobalList.hist_kslk_dep.extend(copy.deepcopy(getGlobalList.kslk_dep))

    getGlobalList.hist_kslk_maintain.extend(copy.deepcopy(getGlobalList.kslk_maintenance_arrival))

    getGlobalList.hist_count_lnj_supply_jsg.append(obj1.count_lanj_supply_jsg)
    getGlobalList.hist_count_kslk_supply_jsg.append(obj1.count_kslk_supply_jsg)

    getGlobalList.hist_count_lnj_supply_balco.append(obj1.count_lanj_supply_balco)
    getGlobalList.hist_count_kslk_supply_balco.append(obj1.count_kslk_supply_balco)

    getGlobalList.hist_count_balcoDep_not_lnjArr.append(obj1.count_balcoDep_not_lnjArr)
    getGlobalList.hist_count_jsgDep_not_lnjArr.append(obj1.count_jsgDep_not_lnjArr)

    getGlobalList.hist_flag.append(copy.deepcopy(obj1.flag))
    getGlobalList.hist_plant_record.append(copy.deepcopy(obj1.plantdates))

    getGlobalList.hist_plant1_demand.append(obj1.plant1_demand)
    getGlobalList.hist_plant2_demand.append(obj1.plant2_demand)

    getGlobalList.hist_hold_records.append(copy.deepcopy(obj1.holdlist))

    obj1.plant1_arrival.clear()
    obj1.plant2_arrival.clear()
    obj1.plantdates.clear()

    getGlobalList.balco_arrival.clear()
    getGlobalList.balco_dep.clear()

    getGlobalList.jsg_arrival.clear()
    getGlobalList.jsg_dep.clear()

    getGlobalList.lnj_arrival.clear()
    getGlobalList.lnj_dep.clear()

    getGlobalList.vizag_arrival.clear()
    getGlobalList.vizag_dep.clear()

    getGlobalList.kslk_arrival.clear()
    getGlobalList.kslk_dep.clear()

    getGlobalList.kslk_maintenance_arrival.clear()


def newPatternSortedDict():
    global obj1,getGlobalList
    tempDic = []
    for val in obj1.sortedDict:
        ls = [val[0],[val[1][1],round(val[1][2],2),val[1][0]]]
        tempDic.append(ls)
    return tempDic


def updateBalcoSortedDict():
    global obj1,getGlobalList
    tempdict = sorted(obj1.sortedDict,key=lambda x: x[1][1])
    templs = []
    for ls in tempdict:
        if ls[1][0] == 'BALCO' and ls[1][1] <= 24:
            templs.append(ls)
    dic = {}
    if len(templs) > 1:
        delay = 0
        for ind,val in enumerate(templs[1:],1):
            rem = templs[ind - 1][1][1] + 12
            arrivalTime = val[1][1]
            if rem > arrivalTime:
                delay += rem - arrivalTime
                obj1.holdlist.append(
                    [val[0],obj1.sys_date,obj1.sys_date + timedelta(hours=delay),'BALCO',obj1.sys_date])
            newval = val[1][1] + delay
            if val[0] not in getGlobalList.btap_in_vizag and newval >= 24:
                btapindex = int(val[0][4:]) - 1
            templs[ind][1][1] += delay
            dic[val[0]] = val[1][1]
    for ind,val in enumerate(obj1.sortedDict):
        if val[0] in dic:
            obj1.sortedDict[ind][1][1] = dic[val[0]]


def updateSortedDict(specialSchedule=None,specialSchedule2=None,specialSchedule3=None,date=None,sdate=None):
    global obj1,getGlobalList

    newdic = []
    ved = []

    if specialSchedule:
        obj1.timeSpend = round((date.hour + (date.minute // 60)),2)
    if specialSchedule2:
        obj1.timeSpend = 24 - round((date.hour + (date.minute // 60)),2)
    if specialSchedule3:
        obj1.timeSpend = round((date.hour + (date.minute // 60)),2) - round((sdate.hour + (sdate.minute // 60)),2)

    y = 0
    if specialSchedule or specialSchedule2 or specialSchedule3:
        y = obj1.timeSpend
    else:
        y = 24 - y

    for val in obj1.sortedDict:

        if (specialSchedule or specialSchedule2 or specialSchedule3) and val[1][1] - obj1.timeSpend >= 0.00:
            new_val = [val[0],[val[1][0],round(val[1][1] - obj1.timeSpend,2),val[1][2]]]
            ved.append(val[0])
            newdic.append(new_val)
            continue
        elif val[1][1] - 24 >= 0.00:
            new_val = [val[0],[val[1][0],round(val[1][1] - (24 - obj1.timeSpend),2),val[1][2]]]
            ved.append(val[0])
            newdic.append(new_val)

    for val in getGlobalList.lnj_arrival:
        if val[0] in ved:
            continue
        if val[0] in getGlobalList.btap_in_balco:

            d1 = obj1.tat_dict['LNJ-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict['KSLK-BALCO'] + obj1.tat_dict[
                'BALCO-Plant'] + obj1.tat_dict['BALCO-KSLK']
            d2 = obj1.btap_txr_details[val[0]] - (val[1] + timedelta(days=obj1.tat_dict['LNJ-Plant']))

            Total_hours = d2.days * 24 + d2.seconds // 3600 + 5 * 24
            goingForLoading = d1 * 24 < Total_hours

            if obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip:
                obj1.count_lanj_supply_balco += 1
                tat = round(((val[1].hour + (val[1].minute / 60)) + (
                        (obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict['LNJ-BALCO']) * 24)) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],['BALCO',tat,'L']]
                ved.append(val[0])
                newdic.append(ls)
            else:
                tat = round(((val[1].hour + (val[1].minute / 60)) + ((obj1.tat_dict['LNJ-KSLK']) * 24)) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                btap_index = int(val[0][4:]) - 1
                if obj1.flag[btap_index] == 0 and not goingForLoading:
                    ls = [val[0],['KSLK-TXR',tat,'E']]
                else:
                    ls = [val[0],['KSLK',tat,'E']]
                ved.append(val[0])
                newdic.append(ls)
        else:

            d1 = obj1.tat_dict['LNJ-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict['KSLK-JSG'] + obj1.tat_dict[
                'JSG-Plant'] + obj1.tat_dict['JSG-KSLK']
            d2 = obj1.btap_txr_details[val[0]] - (val[1] + timedelta(days=obj1.tat_dict['LNJ-Plant']))
            Total_hours = d2.days * 24 + d2.seconds // 3600 + 5 * 24
            goingForLoading = d1 * 24 < Total_hours
            if obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip:
                obj1.count_lanj_supply_jsg += 1
                tat = round(((val[1].hour + (val[1].minute / 60)) + (
                        (obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict['LNJ-JSG']) * 24)) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],['JSG',tat,'L']]
                ved.append(val[0])
                newdic.append(ls)
            else:
                # lnj to kslk
                tat = round(((val[1].hour + (val[1].minute / 60)) + ((obj1.tat_dict['LNJ-KSLK']) * 24)) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                btap_index = int(val[0][4:]) - 1
                if obj1.flag[btap_index] == 0 and not goingForLoading:
                    ls = [val[0],['KSLK-TXR',tat,'E']]
                else:
                    ls = [val[0],['KSLK',tat,'E']]
                ved.append(val[0])
                newdic.append(ls)

    for val in getGlobalList.lnj_dep:
        if val[0] in ved:
            continue
        if val[0] in getGlobalList.btap_in_balco:
            if obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip:
                obj1.count_lanj_supply_balco += 1
                tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['LNJ-BALCO'] * 24)) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],[val[2],tat,'L']]
                ved.append(val[0])
                newdic.append(ls)
        else:
            if obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip:
                obj1.count_lanj_supply_jsg += 1
                tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['LNJ-JSG'] * 24)) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],[val[2],tat,'L']]
                ved.append(val[0])
                newdic.append(ls)

    for val in getGlobalList.vizag_arrival:
        if val[0] in ved:
            continue
        tat = round((obj1.tat_dict['TXR'] * 24 - (val[1].hour + (val[1].minute / 60))),2)
        if tat <= 0:
            edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
            tat += edate * 24
        ls = [val[0],['TXR',tat,'E']]
        ved.append(val[0])
        newdic.append(ls)

    for val in getGlobalList.vizag_dep:
        if val[0] in ved:
            continue
        tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['Vizag-KSLK']) * 24) - y,2)
        if tat <= 0:
            edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
            tat += edate * 24
        ls = [val[0],['KSLK',tat,'E']]
        ved.append(val[0])
        newdic.append(ls)

    for val in getGlobalList.kslk_arrival:
        if val[0] in ved:
            continue
        for ind in range(obj1.sys_date.day - 1,len(obj1.cylon_capacity)):
            obj1.cylon_capacity[ind] -= 3
        if val[0] in getGlobalList.btap_in_balco:
            if obj1.count_kslk_supply_balco < obj1.kslk_to_balco_trip:
                obj1.count_kslk_supply_balco += 1
                tat = round(((val[1].hour + (val[1].minute / 60)) + (
                        (obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict['KSLK-BALCO']) * 24)) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],['BALCO',tat,'L']]
                ved.append(val[0])
                newdic.append(ls)
            else:
                tat = obj1.tat_dict['KSLK-LNJ']
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],['LNJ',tat,'L']]
                ved.append(val[0])
                newdic.append(ls)
        else:
            if obj1.count_kslk_supply_jsg < obj1.kslk_to_jsg_trip:
                obj1.count_kslk_supply_jsg += 1

                tat = round(((val[1].hour + (val[1].minute / 60)) + (
                        (obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict['KSLK-JSG']) * 24)) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],['JSG',tat,'L']]
                ved.append(val[0])
                newdic.append(ls)
            else:
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],['LNJ',tat,'L']]
                ved.append(val[0])
                newdic.append(ls)

    for val in getGlobalList.kslk_maintenance_arrival:
        if val[0] in ved:
            continue
        tat = round((obj1.tat_dict['TXR'] * 24 - (24 - (val[1].hour + (val[1].minute / 60)))),2)
        if tat <= 0:
            edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
            tat += edate * 24
        ls = [val[0],['TXR',tat,'E']]
        ved.append(val[0])
        newdic.append(ls)

    for val in getGlobalList.kslk_dep:
        if val[0] in ved:
            continue
        if val[0] in getGlobalList.btap_in_balco:
            if obj1.count_kslk_supply_balco < obj1.kslk_to_balco_trip:
                obj1.count_kslk_supply_balco += 1
                tempvalue = (val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['KSLK-BALCO'] * 24) + (
                    val[1].day) * 24
                tat = round((tempvalue - (obj1.sys_date.day) * 24) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],[val[2],tat,'L']]
                ved.append(val[0])
                newdic.append(ls)
            else:
                tat = obj1.tat_dict['KSLK-LNJ']
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],['LNJ',tat,'L']]
                ved.append(val[0])
                newdic.append(ls)
        else:
            if obj1.count_kslk_supply_jsg < obj1.kslk_to_jsg_trip:
                obj1.count_kslk_supply_jsg += 1
                tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['KSLK-JSG'] * 24) + (
                        val[1].day - obj1.sys_date.day) * 24) - y,2)
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],[val[2],tat,'L']]
                ved.append(val[0])
                newdic.append(ls)
            else:
                tat = obj1.tat_dict['KSLK-LNJ']
                if tat <= 0:
                    edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                    tat += edate * 24
                ls = [val[0],['LNJ',tat,'L']]
                ved.append(val[0])
                newdic.append(ls)

    for val in getGlobalList.jsg_arrival:
        if val[0] in ved:
            continue
        dest = jsg_check_arrival_update(val[0],val[1])
        if dest == "LNJ":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-LNJ']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['LNJ',tat,'E']]
            obj1.count_jsgDep_not_lnjArr += 1
            obj1.ls_jsg_to_lnj_btap.append([val[0],val[1] + timedelta(days=obj1.tat_dict['JSG-Plant']),'LNJ'])
            obj1.ls_jsg_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_jsg_to_lnj_btap])]

        elif dest == "Vizag":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-Vizag']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['Vizag',tat,'E']]

        elif dest == "KSLK":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-KSLK']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['KSLK',tat,'E']]

        elif dest == 'KSLK-TXR':
            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-KSLK']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['KSLK-TXR',tat,'E']]

        elif dest == "LNJ-HOLD":
            delay = 0
            for val2 in obj1.ls_jsg_to_lnj_btap:
                jsg_dep_date = val[1] + timedelta(days=obj1.tat_dict['JSG-Plant'])
                if val2[1] >= jsg_dep_date - timedelta(hours=1.02 * 24) and (
                        val2[1] - timedelta(hours=1.02 * 24)).month == jsg_dep_date.month and val2[1] <= jsg_dep_date:
                    delay = val2[1].day * 24 + val2[1].hour + 24 - (jsg_dep_date.day * 24 + jsg_dep_date.hour)
                    obj1.holdlist.append(
                        [val2[0],val[1],val[1] + timedelta(hours=delay),'JSG',obj1.sys_date])
                    obj1.count_jsgDep_not_lnjArr += 1
            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-LNJ']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['LNJ',tat + delay,'E']]

            obj1.ls_jsg_to_lnj_btap.append(
                [val[0],val[1] + timedelta(days=obj1.tat_dict['JSG-Plant']) + timedelta(hours=delay),'LNJ'])
            obj1.ls_jsg_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_jsg_to_lnj_btap])]

        ved.append(val[0])
        newdic.append(ls)

    for val in getGlobalList.jsg_dep:
        if val[0] in ved:
            continue

        dest = val[2]
        if dest == "LNJ":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['JSG-LNJ'] * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['LNJ',tat,'E']]
            obj1.count_jsgDep_not_lnjArr += 1
            obj1.ls_jsg_to_lnj_btap.append([val[0],val[1],'LNJ'])
            obj1.ls_jsg_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_jsg_to_lnj_btap])]

        elif dest == "Vizag":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['JSG-Vizag'] * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['Vizag',tat,'E']]

        elif dest == 'KSLK-TXR':
            tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['JSG-KSLK'] * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['KSLK-TXR',tat,'E']]

        else:
            tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['JSG-KSLK'] * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['KSLK',tat,'E']]
        ved.append(val[0])
        newdic.append(ls)

    for val in getGlobalList.balco_arrival:
        if val[0] in ved:
            continue
        dest = balco_check_arrival_update(val[0],val[1])
        if dest == "LNJ":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-LNJ']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['LNJ',tat,'E']]
            obj1.count_balcoDep_not_lnjArr += 1
            obj1.ls_balco_to_lnj_btap.append([val[0],val[1] + timedelta(days=obj1.tat_dict['BALCO-Plant']),'LNJ'])
            obj1.ls_balco_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_balco_to_lnj_btap])]
        elif dest == "Vizag":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-Vizag']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['Vizag',tat,'E']]
        elif dest == "KSLK":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-KSLK']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['KSLK',tat,'E']]
        elif dest == 'KSLK-TXR':
            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-KSLK']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['KSLK-TXR',tat,'E']]
        elif dest == "LANJ-HOLD":
            delay = 0
            for val2 in obj1.ls_balco_to_lnj_btap:
                balco_dep_date = val[1] + timedelta(days=obj1.tat_dict['BALCO-Plant'])
                if balco_dep_date - timedelta(hours=obj1.tat_dict['LNJ-BALCO'] * 24) <= val2[1] <= balco_dep_date and (
                        val2[1] - timedelta(hours=obj1.tat_dict['LNJ-BALCO'] * 24)).month == balco_dep_date.month:
                    delay = val2[1].day * 24 + val2[1].hour + 24 - (balco_dep_date.day * 24 + balco_dep_date.hour)
                    btapDelaySortedDict(val[0],delay)
                    obj1.holdlist.append(
                        [val2[0],val[1],val[1] + timedelta(hours=delay),'BALCO',obj1.sys_date])

            tat = round(((val[1].hour + (val[1].minute / 60)) + (
                    (obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-LNJ']) * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['LNJ',tat + delay,'E']]
            obj1.count_balcoDep_not_lnjArr += 1
            obj1.ls_balco_to_lnj_btap.append(
                [val[0],val[1] + timedelta(days=obj1.tat_dict['BALCO-Plant']) + + timedelta(hours=delay),'LNJ'])
            obj1.ls_balco_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_balco_to_lnj_btap])]

        ved.append(val[0])
        newdic.append(ls)

    for val in getGlobalList.balco_dep:
        if val[0] in ved:
            continue
        dest = val[2]
        if dest == "LNJ":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['BALCO-LNJ'] * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['LNJ',tat,'E']]
            obj1.count_balcoDep_not_lnjArr += 1
            obj1.ls_balco_to_lnj_btap.append([val[0],val[1],'LNJ'])
            obj1.ls_balco_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_balco_to_lnj_btap])]

        elif dest == "Vizag":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['BALCO-Vizag'] * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['Vizag',tat,'E']]
        elif dest == "KSLK":
            tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['BALCO-KSLK'] * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['KSLK',tat,'E']]
        else:
            tat = round(((val[1].hour + (val[1].minute / 60)) + (obj1.tat_dict['BALCO-KSLK'] * 24)) - y,2)
            if tat <= 0:
                edate = monthrange(obj1.sys_date.year,obj1.sys_date.month)[1]
                tat += edate * 24
            ls = [val[0],['KSLK-TXR',tat,'E']]
        ved.append(val[0])
        newdic.append(ls)
    return newdic


def lanj_to_jsg(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    jsg_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(jsg_arrival_date,date)
    if x == 0:
        return
    getGlobalList.jsg_arrival.append([btap_id,jsg_arrival_date])
    jsg_check(btap_id,jsg_arrival_date,obj1.flag,date)


def lanj_to_balco(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    balco_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(balco_arrival_date,date)
    if x == 0:
        return
    getGlobalList.balco_arrival.append([btap_id,balco_arrival_date])
    balco_check(btap_id,balco_arrival_date,obj1.flag,date)


def jsg_check_arrival_update(btap_id,jsg_arrival_date):
    global obj1,getGlobalList
    jsg_unload_time = obj1.tat_dict['JSG-Plant']
    jsg_dep_date = jsg_arrival_date + timedelta(hours=jsg_unload_time * 24)

    get_btap_txr_date = obj1.btap_txr_details[btap_id]
    day_difference = get_btap_txr_date - jsg_dep_date
    diff_days = day_difference.days * 24
    diff_hours = day_difference.seconds // 3600
    total_hours = diff_days + diff_hours

    countLnj = 0
    for val in getGlobalList.hist_lnj_arrival:
        countLnj += len(val)
    obj1.count_jsg_to_lnj = 0

    obj1.ls_jsg_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_jsg_to_lnj_btap])]

    for val in obj1.ls_jsg_to_lnj_btap:
        if val[1] >= jsg_dep_date - timedelta(hours=1.02 * 24) and val[1].month == jsg_dep_date.month and val[
            1] < jsg_dep_date:
            obj1.count_jsg_to_lnj += 1
    btap = int(btap_id[4:]) - 1

    if btap_id in getGlobalList.btap_in_vizag:
        tat_for_lanj_trip_and_txr = obj1.tat_dict['JSG-LNJ'] + obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict['LNJ-JSG'] + \
                                    obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-Vizag']
        tat_for_kslk_trip_and_txr = obj1.tat_dict['JSG-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict[
            'KSLK-JSG'] + obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-Vizag']

        vizagTest = obj1.flag[btap] == 0 and (total_hours + 120 < tat_for_lanj_trip_and_txr * 24)

        lanjTest = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj < 3)) or (
                           (obj1.flag[btap] == 1) and (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                           obj1.count_jsg_to_lnj < 3))

        holdTest = ((obj1.flag[btap] == 1) and (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                obj1.count_jsg_to_lnj >= 3)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3))

        kslkTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3)) or (
                obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip)) and (
                            obj1.count_kslk_supply_jsg <= obj1.kslk_to_jsg_trip)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           (obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) or (
                           (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                           obj1.count_jsg_to_lnj >= 3)) and (
                                   tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                   obj1.count_kslk_supply_jsg <= obj1.kslk_to_jsg_trip)))

        holdAndBreakTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3)) or (
                obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip)) and (
                                    obj1.count_kslk_supply_jsg >= obj1.kslk_to_jsg_trip)) or (
                                   (obj1.flag[btap] == 0) and (
                                   total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and ((
                                                                                                     obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) or (
                                                                                                     (
                                                                                                             obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                                                                                                             obj1.count_jsg_to_lnj >= 3)) and (
                                                                                                     tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                                                                                     obj1.count_kslk_supply_jsg > obj1.kslk_to_jsg_trip)))

        holdTillTxr = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                (obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) and (
                tat_for_kslk_trip_and_txr * 24 <= total_hours + 120)) or ((obj1.flag[btap] == 0) and (
                total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                                                                                  obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                                                                                  obj1.count_jsg_to_lnj >= 3) and (
                                                                                  tat_for_kslk_trip_and_txr * 24 > total_hours + 120)))
        if vizagTest:
            # Going to vizag for maintenance
            return "Vizag"

        elif lanjTest:
            # Going to lanj for Loading
            return "LNJ"

        elif kslkTest:
            # Going to kslk for Loading
            return "KSLK"

        elif holdTest:
            # Hold at JSG
            return "LNJ-HOLD"

        elif holdAndBreakTest:
            # Hold and break
            return "Exit"

        elif holdTillTxr:
            # Hold till TXR
            return "Vizag"
        else:
            return "Exit"

    else:
        tat_for_lanj_trip_and_txr = obj1.tat_dict['JSG-LNJ'] + obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict['LNJ-JSG'] + \
                                    obj1.tat_dict['JSG-KSLK']
        tat_for_kslk_trip_and_txr = obj1.tat_dict['JSG-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict[
            'KSLK-JSG'] + obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-KSLK']

        kslkMaintanenceTest = obj1.flag[btap] == 0 and (total_hours + 120 < tat_for_lanj_trip_and_txr * 24)

        lanjTest = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj < 3)) or (
                           (obj1.flag[btap] == 1) and (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                           obj1.count_jsg_to_lnj < 3))

        kslkTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3)) or (
                obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip)) and (
                            obj1.count_kslk_supply_jsg < obj1.kslk_to_jsg_trip)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           (obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) or (
                           (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                           obj1.count_jsg_to_lnj >= 3)) and (
                                   tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                   obj1.count_kslk_supply_jsg < obj1.kslk_to_jsg_trip)))

        holdTest = ((obj1.flag[btap] == 1) and (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                obj1.count_jsg_to_lnj >= 3)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3))

        holdAndBreakTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3)) or (
                obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip)) and (
                                    obj1.count_kslk_supply_jsg >= obj1.kslk_to_jsg_trip)) or (
                                   (obj1.flag[btap] == 0) and (
                                   total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                                           (obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) or (
                                           (
                                                   obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                                                   obj1.count_jsg_to_lnj >= 3)) and (
                                                   tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                                   obj1.count_kslk_supply_jsg >= obj1.kslk_to_jsg_trip)))

        holdTillTxr = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                (obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) and (
                tat_for_kslk_trip_and_txr * 24 <= total_hours + 120)) or ((obj1.flag[btap] == 0) and (
                total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                                                                                  obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                                                                                  obj1.count_jsg_to_lnj >= 3) and (
                                                                                  tat_for_kslk_trip_and_txr * 24 > total_hours + 120)))

        if kslkMaintanenceTest:
            return "KSLK-TXR"
        elif lanjTest:
            # Going to lanj for Loading
            return "LNJ"
        elif kslkTest:
            # Going to kslk for Loading
            return "KSLK"

        elif holdTest:
            # Hold at JSG
            return "LNJ-HOLD"

        elif holdAndBreakTest:
            # Hold and break
            return "Exit"

        elif holdTillTxr:
            return "KSLK-TXR"

        else:
            return "Exit"


def balco_check_arrival_update(btap_id,balco_arrival_date):
    global obj1,getGlobalList
    balco_unload_time = obj1.tat_dict['BALCO-Plant']
    balco_dep_date = balco_arrival_date + timedelta(hours=balco_unload_time * 24)

    get_btap_txr_date = obj1.btap_txr_details[btap_id]
    day_difference = get_btap_txr_date - balco_dep_date
    diff_days = day_difference.days * 24
    diff_hours = day_difference.seconds // 3600
    total_hours = diff_days + diff_hours

    countLnj = 0
    for val in getGlobalList.hist_lnj_arrival:
        countLnj += len(val)

    obj1.count_balco_to_lnj = 0

    obj1.ls_balco_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_balco_to_lnj_btap])]
    for val in obj1.ls_balco_to_lnj_btap:
        if val[1] >= balco_dep_date - timedelta(hours=1.02 * 24) and val[1].month == balco_dep_date.month and val[
            1] < balco_dep_date:
            obj1.count_balco_to_lnj += 1
    btap = int(btap_id[4:]) - 1

    if btap_id in getGlobalList.btap_in_vizag:

        tat_for_lanj_trip_and_txr = obj1.tat_dict['BALCO-LNJ'] + obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict[
            'LNJ-BALCO'] + \
                                    obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-Vizag']
        tat_for_kslk_trip_and_txr = obj1.tat_dict['BALCO-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict[
            'KSLK-BALCO'] + obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-Vizag']
        vizagTest = obj1.flag[btap] == 0 and (total_hours + 120 < tat_for_lanj_trip_and_txr * 24)

        lanjTest = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj < 3)) or (
                           (obj1.flag[btap] == 1) and (obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                           obj1.count_balco_to_lnj < 3))

        kslkTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj >= 3)) or (
                obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip)) and (
                            obj1.count_kslk_supply_balco <= obj1.kslk_to_jsg_trip)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           (obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) or (
                           (obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                           obj1.count_balco_to_lnj >= 3)) and (
                                   tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                   obj1.count_kslk_supply_balco <= obj1.kslk_to_jsg_trip)))

        holdTest = ((obj1.flag[btap] == 1) and (obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip) and (
                obj1.count_balco_to_lnj >= 3)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip) and (
                                   obj1.count_balco_to_lnj >= 3))

        holdAndBreakTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj >= 3)) or (
                obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip)) and (
                                    obj1.count_kslk_supply_balco >= obj1.kslk_to_jsg_trip)) or (
                                   (obj1.flag[btap] == 0) and (
                                   total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and ((
                                                                                                     obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) or (
                                                                                                     (
                                                                                                             obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                                                                                                             obj1.count_balco_to_lnj >= 3)) and (
                                                                                                     tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                                                                                     obj1.count_kslk_supply_balco >= obj1.kslk_to_jsg_trip)))

        holdTillTxr = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                (obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) and (
                tat_for_kslk_trip_and_txr * 24 <= total_hours + 120)) or ((obj1.flag[btap] == 0) and (
                total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                                                                                  obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                                                                                  obj1.count_balco_to_lnj >= 3) and (
                                                                                  tat_for_kslk_trip_and_txr * 24 >= total_hours + 120)))
        if vizagTest:
            # Going to vizag for maintenance
            return "Vizag"

        elif lanjTest:
            # Going to lanj for Loading
            return "LNJ"

        elif kslkTest:
            # Going to kslk for Loading
            return "KSLK"

        elif holdTest:
            # Hold at JSG
            return "LANJ-HOLD"

        elif holdAndBreakTest:
            # Hold and break
            return "Exit"

        elif holdTillTxr:
            # Hold till TXR
            return "Vizag"
        else:
            return "Exit"

    else:

        tat_for_lanj_trip_and_txr = obj1.tat_dict['BALCO-LNJ'] + obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict[
            'LNJ-BALCO'] + obj1.tat_dict['BALCO-KSLK']
        tat_for_kslk_trip_and_txr = obj1.tat_dict['BALCO-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict[
            'KSLK-BALCO'] + obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-KSLK']

        kslkMaintanenceTest = obj1.flag[btap] == 0 and (total_hours + 120 < tat_for_lanj_trip_and_txr * 24)
        lanjTest = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj < 3)) or (
                           (obj1.flag[btap] == 1) and (obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                           obj1.count_balco_to_lnj < 3))

        kslkTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj >= 3)) or (
                obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip)) and (
                            obj1.count_kslk_supply_balco < obj1.kslk_to_jsg_trip)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           (obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) or (
                           (obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                           obj1.count_balco_to_lnj >= 3)) and (
                                   tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                   obj1.count_kslk_supply_balco < obj1.kslk_to_jsg_trip)))

        holdTest = ((obj1.flag[btap] == 1) and (obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip) and (
                obj1.count_balco_to_lnj >= 3)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip) and (
                                   obj1.count_balco_to_lnj >= 3))

        holdAndBreakTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj >= 3)) or (
                obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip)) and (
                                    obj1.count_kslk_supply_balco >= obj1.kslk_to_jsg_trip)) or (
                                   (obj1.flag[btap] == 0) and (
                                   total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and ((
                                                                                                     obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) or (
                                                                                                     (
                                                                                                             obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                                                                                                             obj1.count_balco_to_lnj >= 3)) and (
                                                                                                     tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                                                                                     obj1.count_kslk_supply_balco >= obj1.kslk_to_jsg_trip)))

        holdTillTxr = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                (obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) and (
                tat_for_kslk_trip_and_txr * 24 <= total_hours + 120)) or ((obj1.flag[btap] == 0) and (
                total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                                                                                  obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                                                                                  obj1.count_balco_to_lnj >= 3) and (
                                                                                  tat_for_kslk_trip_and_txr * 24 >= total_hours + 120)))

        if kslkMaintanenceTest:
            # Going to kslk for maintenance
            return "KSLK-TXR"

        elif lanjTest:
            # Going to lanj for Loading
            return "LNJ"

        elif kslkTest:
            # Going to kslk for Loading
            return "KSLK"

        elif holdTest:
            # Hold at JSG
            return "LANJ-HOLD"

        elif holdAndBreakTest:
            # Hold and break
            return "Exit"

        elif holdTillTxr:
            # Hold till TXR
            return "KSLK-TXR"

        else:
            return "Exit"


def jsg_to_vizag(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    vizag_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(vizag_arrival_date,date)
    if x == 0:
        return
    getGlobalList.vizag_arrival.append([btap_id,vizag_arrival_date])
    vizag_dep_date = vizag_arrival_date + timedelta(hours=obj1.tat_dict['TXR'] * 24)
    x = check(vizag_dep_date,date)
    if x == 0:
        return
    getGlobalList.vizag_dep.append([btap_id,vizag_dep_date,'KSLK'])
    vizag_to_kslk(btap_id,vizag_dep_date,obj1.tat_dict['Vizag-KSLK'],obj1.flag,date)


def balco_to_vizag(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    vizag_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(vizag_arrival_date,date)
    if x == 0:
        return
    getGlobalList.vizag_arrival.append([btap_id,vizag_arrival_date])
    vizag_dep_date = vizag_arrival_date + timedelta(hours=obj1.tat_dict['TXR'] * 24)
    x = check(vizag_dep_date,date)
    if x == 0:
        return
    getGlobalList.vizag_dep.append([btap_id,vizag_dep_date,'KSLK'])
    vizag_to_kslk(btap_id,vizag_dep_date,obj1.tat_dict['Vizag-KSLK'],obj1.flag,date)


def jsg_to_kslk_maintain(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    kslk_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(kslk_arrival_date,date)
    if x == 0:
        return
    getGlobalList.kslk_maintenance_arrival.append([btap_id,kslk_arrival_date])
    kslk_dep_date = kslk_arrival_date + timedelta(hours=obj1.tat_dict['TXR'] * 24)
    getGlobalList.kslk_dep.append([btap_id,kslk_dep_date,'KSLK'])
    tat_kslk_jsg = obj1.tat_dict['KSLK-JSG']
    kslk_to_jsg(btap_id,kslk_dep_date,tat_kslk_jsg,obj1.flag,date)


def balco_to_kslk_maintain(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    kslk_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(kslk_arrival_date,date)
    if x == 0:
        return
    getGlobalList.kslk_maintenance_arrival.append([btap_id,kslk_arrival_date])
    kslk_dep_date = kslk_arrival_date + timedelta(hours=obj1.tat_dict['TXR'] * 24)
    getGlobalList.kslk_dep.append([btap_id,kslk_dep_date,'KSLK'])
    tat_kslk_balco = obj1.tat_dict['KSLK-BALCO']
    kslk_to_balco(btap_id,kslk_dep_date,tat_kslk_balco,obj1.flag,date)


def jsg_to_lanj(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    lnj_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(lnj_arrival_date,date)
    if x == 0:
        return
    getGlobalList.lnj_arrival.append([btap_id,lnj_arrival_date])
    lnj_dep_date = lnj_arrival_date + timedelta(hours=obj1.tat_dict['LNJ-Plant'] * 24)
    getGlobalList.lnj_dep.append([btap_id,lnj_dep_date,'JSG'])
    tat_lnj_jsg = obj1.tat_dict['LNJ-JSG']
    lanj_to_jsg(btap_id,lnj_dep_date,tat_lnj_jsg,obj1.flag,date)


def balco_to_lanj(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    lnj_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(lnj_arrival_date,date)
    if x == 0:
        return
    getGlobalList.lnj_arrival.append([btap_id,lnj_arrival_date])
    lnj_dep_date = lnj_arrival_date + timedelta(hours=obj1.tat_dict['LNJ-Plant'] * 24)
    getGlobalList.lnj_dep.append([btap_id,lnj_dep_date,'BALCO'])
    tat_lnj_balco = obj1.tat_dict['LNJ-BALCO']
    lanj_to_balco(btap_id,lnj_dep_date,tat_lnj_balco,obj1.flag,date)


def kslk_to_jsg(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    jsg_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(jsg_arrival_date,date)
    if x == 0:
        return
    getGlobalList.jsg_arrival.append([btap_id,jsg_arrival_date])
    jsg_check(btap_id,jsg_arrival_date,obj1.flag,date)


def kslk_to_balco(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    balco_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(balco_arrival_date,date)
    if x == 0:
        return
    getGlobalList.balco_arrival.append([btap_id,balco_arrival_date])
    balco_check(btap_id,balco_arrival_date,obj1.flag,date)


def vizag_to_kslk(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    kslk_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(kslk_arrival_date,date)
    if x == 0:
        return
    getGlobalList.kslk_arrival.append([btap_id,kslk_arrival_date])
    kslk_dep_date = kslk_arrival_date + timedelta(hours=obj1.tat_dict['KSLK-Plant'] * 24)
    getGlobalList.kslk_dep.append([btap_id,kslk_dep_date,'JSG'])
    tat_kslk_jsg = obj1.tat_dict['KSLK-JSG']
    kslk_to_jsg(btap_id,kslk_dep_date,tat_kslk_jsg,obj1.flag,date)


def jsg_to_kslk(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0

    kslk_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(kslk_arrival_date,date)
    if x == 0:
        return
    getGlobalList.kslk_arrival.append([btap_id,kslk_arrival_date])
    kslk_dep_date = kslk_arrival_date + timedelta(hours=obj1.tat_dict['KSLK-Plant'] * 24)
    getGlobalList.kslk_dep.append([btap_id,kslk_dep_date,'JSG'])
    tat_kslk_jsg = obj1.tat_dict['KSLK-JSG']
    kslk_to_jsg(btap_id,kslk_dep_date,tat_kslk_jsg,obj1.flag,date)


def balco_to_kslk(btap_id,sys_date,duration,flag,date):
    global obj1,getGlobalList
    if sys_date > date:
        return 0
    kslk_arrival_date = sys_date + timedelta(hours=duration * 24)
    x = check(kslk_arrival_date,date)
    if x == 0:
        return
    getGlobalList.kslk_arrival.append([btap_id,kslk_arrival_date])
    kslk_dep_date = kslk_arrival_date + timedelta(hours=obj1.tat_dict['KSLK-Plant'] * 24)
    getGlobalList.kslk_dep.append([btap_id,kslk_dep_date,'BALCO'])
    tat_kslk_balco = obj1.tat_dict['KSLK-BALCO']
    kslk_to_balco(btap_id,kslk_dep_date,tat_kslk_balco,obj1.flag,date)


def jsg_check(btap_id,jsg_arrival_date,flag,date):
    global obj1,getGlobalList
    if jsg_arrival_date > date:
        return 0
    jsg_unload_time = obj1.tat_dict['JSG-Plant']
    jsg_dep_date = jsg_arrival_date + timedelta(hours=jsg_unload_time * 24)
    get_btap_txr_date = obj1.btap_txr_details[btap_id]
    day_difference = get_btap_txr_date - jsg_dep_date
    diff_days = day_difference.days * 24
    diff_hours = day_difference.seconds // 3600
    total_hours = diff_days + diff_hours

    if btap_id in obj1.jsg_to_lnj_list:
        getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'LANJ'])
        tat_jsg_lnj = obj1.tat_dict['JSG-LNJ']
        jsg_to_lanj(btap_id,jsg_dep_date,tat_jsg_lnj,obj1.flag,date)
        return 0
    elif btap_id in obj1.jsg_to_kslk_ls:
        getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'KSLK'])
        tat_jsg_kslk = obj1.tat_dict['JSG-KSLK']
        jsg_to_kslk(btap_id,jsg_dep_date,tat_jsg_kslk,obj1.flag,date)
        return 0

    countLnj = 0
    for val in getGlobalList.hist_lnj_arrival:
        countLnj += len(val)

    obj1.count_jsg_to_lnj = 0
    obj1.ls_jsg_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_jsg_to_lnj_btap])]
    for val in obj1.ls_jsg_to_lnj_btap:
        if val[1] >= jsg_dep_date - timedelta(hours=1.02 * 24) and (
                val[1] - timedelta(hours=1.02 * 24)).month == jsg_dep_date.month and val[1] <= jsg_dep_date:
            obj1.count_jsg_to_lnj += 1

    btap = int(btap_id[4:]) - 1

    if btap_id in getGlobalList.btap_in_vizag:
        tat_for_lanj_trip_and_txr = obj1.tat_dict['JSG-LNJ'] + obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict['LNJ-JSG'] + \
                                    obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-Vizag']
        tat_for_kslk_trip_and_txr = obj1.tat_dict['JSG-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict[
            'KSLK-JSG'] + obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-Vizag']

        vizagTest = obj1.flag[btap] == 0 and (total_hours + 120 < tat_for_lanj_trip_and_txr * 24)

        lanjTest = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj < 3)) or (
                           (obj1.flag[btap] == 1) and (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                           obj1.count_jsg_to_lnj < 3))

        kslkTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3)) or (
                obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip)) and (
                            obj1.count_kslk_supply_jsg <= obj1.kslk_to_jsg_trip)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           (obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) or (
                           (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                           obj1.count_jsg_to_lnj >= 3)) and (
                                   tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                   obj1.count_kslk_supply_jsg <= obj1.kslk_to_jsg_trip)))

        holdTest = ((obj1.flag[btap] == 1) and (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                obj1.count_jsg_to_lnj >= 3)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3))

        holdAndBreakTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3)) or (
                obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip)) and (
                                    obj1.count_kslk_supply_jsg >= obj1.kslk_to_jsg_trip)) or (
                                   (obj1.flag[btap] == 0) and (
                                   total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and ((
                                                                                                     obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) or (
                                                                                                     (
                                                                                                             obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                                                                                                             obj1.count_jsg_to_lnj >= 3)) and (
                                                                                                     tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                                                                                     obj1.count_kslk_supply_jsg > obj1.kslk_to_jsg_trip)))

        holdTillTxr = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                (obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) and (
                tat_for_kslk_trip_and_txr * 24 <= total_hours + 120)) or ((obj1.flag[btap] == 0) and (
                total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                                                                                  obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                                                                                  obj1.count_jsg_to_lnj >= 3) and (
                                                                                  tat_for_kslk_trip_and_txr * 24 > total_hours + 120)))

        if vizagTest:
            # Going to vizag for maintenance
            getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'Vizag'])
            tat_jsg_vizag = obj1.tat_dict['JSG-Vizag']
            jsg_to_vizag(btap_id,jsg_dep_date,tat_jsg_vizag,obj1.flag,date)

        elif lanjTest:
            # Going to lanj for Loading
            getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'LANJ'])
            tat_jsg_lnj = obj1.tat_dict['JSG-LNJ']
            jsg_to_lanj(btap_id,jsg_dep_date,tat_jsg_lnj,obj1.flag,date)

        elif kslkTest:
            # Going to kslk for Loading
            getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'KSLK'])
            tat_jsg_kslk = obj1.tat_dict['JSG-KSLK']
            jsg_to_kslk(btap_id,jsg_dep_date,tat_jsg_kslk,obj1.flag,date)

        elif holdTest:
            pass
            # Hold at JSG

        elif holdAndBreakTest:
            obj1.hold_list.append([btap_id,obj1.sys_date])
            sys.exit(0)

        elif holdTillTxr:
            # Hold till TXR
            jsg_dep_date = jsg_arrival_date + timedelta(
                days=obj1.btap_txr_details[btap_id].day - obj1.tat_dict['JSG-Vizag'])
            getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'Vizag'])
            tat_jsg_vizag = obj1.tat_dict['JSG-Vizag']
            jsg_to_vizag(btap_id,jsg_dep_date,tat_jsg_vizag,obj1.flag,date)

    else:
        tat_for_lanj_trip_and_txr = obj1.tat_dict['JSG-LNJ'] + obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict['LNJ-JSG'] + \
                                    obj1.tat_dict['JSG-KSLK']
        tat_for_kslk_trip_and_txr = obj1.tat_dict['JSG-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict[
            'KSLK-JSG'] + obj1.tat_dict['JSG-Plant'] + obj1.tat_dict['JSG-KSLK']
        kslkMaintanenceTest = obj1.flag[btap] == 0 and (total_hours + 120 < tat_for_lanj_trip_and_txr * 24)

        lanjTest = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj < 3)) or (
                           (obj1.flag[btap] == 1) and (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                           obj1.count_jsg_to_lnj < 3))

        kslkTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3)) or (
                obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip)) and (
                            obj1.count_kslk_supply_jsg <= obj1.kslk_to_jsg_trip)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           (obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) or (
                           (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                           obj1.count_jsg_to_lnj >= 3)) and (
                                   tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                   obj1.count_kslk_supply_jsg <= obj1.kslk_to_jsg_trip)))

        holdTest = ((obj1.flag[btap] == 1) and (obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                obj1.count_jsg_to_lnj >= 3)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3))

        holdAndBreakTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (obj1.count_jsg_to_lnj >= 3)) or (
                obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip)) and (
                                    obj1.count_kslk_supply_jsg >= obj1.kslk_to_jsg_trip)) or (
                                   (obj1.flag[btap] == 0) and (
                                   total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and ((
                                                                                                     obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) or (
                                                                                                     (
                                                                                                             obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                                                                                                             obj1.count_jsg_to_lnj >= 3)) and (
                                                                                                     tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                                                                                     obj1.count_kslk_supply_jsg > obj1.kslk_to_jsg_trip)))

        holdTillTxr = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                (obj1.count_lanj_supply_jsg >= obj1.lnj_to_jsg_trip) and (
                tat_for_kslk_trip_and_txr * 24 <= total_hours + 120)) or ((obj1.flag[btap] == 0) and (
                total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                                                                                  obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip) and (
                                                                                  obj1.count_jsg_to_lnj >= 3) and (
                                                                                  tat_for_kslk_trip_and_txr * 24 >= total_hours + 120)))

        if kslkMaintanenceTest:
            # Going to kslk for maintenance
            jsg_dep_date = jsg_arrival_date + timedelta(hours=obj1.tat_dict['JSG-Plant'] * 24)
            getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'KSLK-TXR'])
            tat_jsg_kslk = obj1.tat_dict['JSG-KSLK']
            jsg_to_kslk_maintain(btap_id,jsg_dep_date,tat_jsg_kslk,obj1.flag,date)

        elif lanjTest:
            getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'LANJ'])
            tat_jsg_lnj = obj1.tat_dict['JSG-LNJ']
            jsg_to_lanj(btap_id,jsg_dep_date,tat_jsg_lnj,obj1.flag,date)

        elif kslkTest:
            # Going to kslk for Loading
            getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'KSLK'])
            tat_jsg_kslk = obj1.tat_dict['JSG-KSLK']
            jsg_to_kslk(btap_id,jsg_dep_date,tat_jsg_kslk,obj1.flag,date)

        elif holdTest:
            pass

        elif holdAndBreakTest:
            # Hold and break
            obj1.hold_list.append([btap_id,obj1.sys_date])
            sys.exit(0)

        elif holdTillTxr:
            # Hold till TXR
            jsg_dep_date = jsg_arrival_date + timedelta(
                days=obj1.btap_txr_details[btap_id].day - obj1.tat_dict['JSG-KSLK'])
            getGlobalList.jsg_dep.append([btap_id,jsg_dep_date,'KSLK-TXR'])
            tat_jsg_vizag = obj1.tat_dict['JSG-KSLK']
            jsg_to_kslk_maintain(btap_id,jsg_dep_date,tat_jsg_vizag,obj1.flag,date)


def goLNJ():
    global obj1,getGlobalList
    countJSG = 0
    countBalco = 0
    obj1.jsg_to_lnj_list = []
    obj1.balco_to_lnj_list = []
    for val in obj1.sortedDict:
        if val[1][0] == "JSG" and val[1][1] - 24 <= 0.0:
            jsg_arrival_date = obj1.sys_date + timedelta(hours=val[1][1])
            dest = jsg_check_arrival_update(val[0],jsg_arrival_date)
            if dest == "LNJ":
                countJSG += 1
                obj1.jsg_to_lnj_list.append([val[0],val[1]])
        if val[1][0] == "BALCO" and val[1][1] - 24 <= 0.0:
            balco_arrival_date = obj1.sys_date + timedelta(hours=val[1][1])
            dest = balco_check_arrival_update(val[0],balco_arrival_date)
            if dest == "LNJ":
                countBalco += 1
                obj1.balco_to_lnj_list.append([val[0],val[1]])
    obj1.jsg_to_lnj_list_sorted = sorted(obj1.jsg_to_lnj_list,key=lambda x: x[1])
    obj1.balco_to_lnj_list_sorted = sorted(obj1.balco_to_lnj_list,key=lambda x: x[1])
    obj1.jsg_to_lnj_list.clear()
    obj1.balco_to_lnj_list.clear()
    count_total = countBalco + countJSG
    demandJSG = obj1.demand_jsg / (obj1.demand_jsg + obj1.demand_balco)
    p = math.ceil(count_total * demandJSG)
    if p >= countJSG:
        del obj1.jsg_to_lnj_list_sorted[-(countJSG - p):]
        del obj1.balco_to_lnj_list_sorted[-(countBalco - (count_total - p)):]
        obj1.jsg_to_lnj_list = [x for x,y in obj1.jsg_to_lnj_list_sorted]
        obj1.balco_to_lnj_list = [x for x,y in obj1.balco_to_lnj_list_sorted]


def vesselToCylo():
    global obj1,getGlobalList
    for val in obj1.vessel_arrival_date:
        i = 1

        for ind,x in enumerate(obj1.cylon_capacity[val.day - 1:val.day + 3],val.day - 1):
            obj1.cylon_capacity[ind] = x + 7.5 * i
            i += 1
        for ind,x in enumerate(obj1.cylon_capacity[val.day + 3:],val.day + 3):
            obj1.cylon_capacity[ind] = x + 30


def goKSLK():
    global obj1,getGlobalList
    obj1.jsg_to_kslk_ls = []
    obj1.balco_to_kslk_ls = []
    for val in obj1.sortedDict:
        if val[1][0] == "JSG" and val[1][1] - 24 <= 0.0:
            jsg_arrival_date = obj1.sys_date + timedelta(hours=val[1][1])
            dest = jsg_check_arrival_update(val[0],jsg_arrival_date)
            if dest == "KSLK" and obj1.cylon_capacity[obj1.sys_date.day - 1] > 0:
                obj1.jsg_to_kslk_ls.append(val[0])
        if val[1][0] == "BALCO" and val[1][1] - 24 <= 0.0:
            balco_arrival_date = obj1.sys_date + timedelta(hours=val[1][1])
            dest = balco_check_arrival_update(val[0],balco_arrival_date)

            if dest == "KSLK" and obj1.cylon_capacity[obj1.sys_date.day - 1] > 0:
                obj1.balco_to_kslk_ls.append(val[0])


def balco_check(btap_id,balco_arrival_date,flag,date):
    global obj1,getGlobalList
    if balco_arrival_date > date:
        return 0

    balco_unload_time = obj1.tat_dict['BALCO-Plant']
    balco_dep_date = balco_arrival_date + timedelta(hours=balco_unload_time * 24)
    get_btap_txr_date = obj1.btap_txr_details[btap_id]
    day_difference = get_btap_txr_date - balco_dep_date
    diff_days = day_difference.days * 24
    diff_hours = day_difference.seconds // 3600
    total_hours = diff_days + diff_hours

    if btap_id in obj1.ls_balco_to_lnj_btap:
        getGlobalList.balco_dep.append([btap_id,balco_dep_date,'LANJ'])
        tat_balco_lnj = obj1.tat_dict['BALCO-LNJ']
        balco_to_lanj(btap_id,balco_dep_date,tat_balco_lnj,obj1.flag,date)
        return 0
    elif btap_id in obj1.balco_to_kslk_ls:
        getGlobalList.balco_dep.append([btap_id,balco_dep_date,'KSLK'])
        tat_balco_kslk = obj1.tat_dict['BALCO-KSLK']
        balco_to_kslk(btap_id,balco_dep_date,tat_balco_kslk,obj1.flag,date)
        return 0

    countLnj = 0
    for val in getGlobalList.hist_lnj_arrival:
        countLnj += len(val)

    obj1.count_balco_to_lnj = 0

    obj1.ls_balco_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_balco_to_lnj_btap])]
    for val in obj1.ls_balco_to_lnj_btap:
        if val[1] >= balco_dep_date - timedelta(hours=1.02 * 24) and (
                val[1] - timedelta(hours=1.02 * 24)).month == balco_dep_date.month and val[
            1] <= balco_dep_date:
            obj1.count_balco_to_lnj += 1
    btap = int(btap_id[4:]) - 1

    if (btap_id in getGlobalList.btap_in_vizag):
        tat_for_lanj_trip_and_txr = obj1.tat_dict['BALCO-LNJ'] + obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict[
            'LNJ-BALCO'] + \
                                    obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-Vizag']
        tat_for_kslk_trip_and_txr = obj1.tat_dict['BALCO-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict[
            'KSLK-BALCO'] + obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-Vizag']

        vizagTest = obj1.flag[btap] == 0 and (total_hours + 120 < tat_for_lanj_trip_and_txr * 24)

        lanjTest = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj < 3)) or (
                           (obj1.flag[btap] == 1) and (obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                           obj1.count_balco_to_lnj < 3))

        holdTest = ((obj1.flag[btap] == 1) and (obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip) and (
                obj1.count_balco_to_lnj >= 3)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip) and (
                                   obj1.count_balco_to_lnj >= 3))

        kslkTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj >= 3)) or (
                obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip)) and (
                            obj1.count_kslk_supply_balco <= obj1.kslk_to_jsg_trip)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           (obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) or (
                           (obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                           obj1.count_balco_to_lnj >= 3)) and (
                                   tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                   obj1.count_kslk_supply_balco <= obj1.kslk_to_jsg_trip)))

        holdAndBreakTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj >= 3)) or (
                obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip)) and (
                                    obj1.count_kslk_supply_balco >= obj1.kslk_to_jsg_trip)) or (
                                   (obj1.flag[btap] == 0) and (
                                   total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and ((
                                                                                                     obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) or (
                                                                                                     (
                                                                                                             obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                                                                                                             obj1.count_balco_to_lnj >= 3)) and (
                                                                                                     tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                                                                                     obj1.count_kslk_supply_balco >= obj1.kslk_to_jsg_trip)))

        holdTillTxr = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                (obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) and (
                tat_for_kslk_trip_and_txr * 24 <= total_hours + 120)) or ((obj1.flag[btap] == 0) and (
                total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                                                                                  obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                                                                                  obj1.count_balco_to_lnj >= 3) and (
                                                                                  tat_for_kslk_trip_and_txr * 24 > total_hours + 120)))

        if vizagTest:
            # Going to vizag for maintenance
            getGlobalList.balco_dep.append([btap_id,balco_dep_date,'Vizag'])
            tat_balco_vizag = obj1.tat_dict['BALCO-Vizag']
            #         obj1.flag[btap] = 1
            balco_to_vizag(btap_id,balco_dep_date,tat_balco_vizag,obj1.flag,date)

        elif lanjTest:
            # Going to lanj for Loading
            getGlobalList.balco_dep.append([btap_id,balco_dep_date,'LANJ'])
            tat_balco_lnj = obj1.tat_dict['BALCO-LNJ']
            balco_to_lanj(btap_id,balco_dep_date,tat_balco_lnj,obj1.flag,date)

        elif kslkTest:
            # Going to kslk for Loading
            getGlobalList.balco_dep.append([btap_id,balco_dep_date,'KSLK'])
            tat_balco_kslk = obj1.tat_dict['BALCO-KSLK']
            balco_to_kslk(btap_id,balco_dep_date,tat_balco_kslk,obj1.flag,date)

        elif holdTest:
            # Hold at Balco
            pass

        elif holdAndBreakTest:
            # Hold and break
            obj1.hold_list.append([btap_id,obj1.sys_date])

        elif holdTillTxr:
            # Hold till TXR
            balco_dep_date = balco_arrival_date + timedelta(
                days=obj1.btap_txr_details[btap_id].day - obj1.tat_dict['BALCO-Vizag'])
            getGlobalList.balco_dep.append([btap_id,balco_dep_date,'Vizag'])
            tat_balco_vizag = obj1.tat_dict['BALCO-Vizag']
            balco_to_vizag(btap_id,balco_dep_date,tat_balco_vizag,obj1.flag,date)

    else:
        tat_for_lanj_trip_and_txr = obj1.tat_dict['BALCO-LNJ'] + obj1.tat_dict['LNJ-Plant'] + obj1.tat_dict[
            'LNJ-BALCO'] + \
                                    obj1.tat_dict['BALCO-KSLK']
        tat_for_kslk_trip_and_txr = obj1.tat_dict['BALCO-KSLK'] + obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict[
            'KSLK-BALCO'] + obj1.tat_dict['BALCO-Plant'] + obj1.tat_dict['BALCO-KSLK']

        kslkMaintanenceTest = obj1.flag[btap] == 0 and (total_hours + 120 < tat_for_lanj_trip_and_txr * 24)

        lanjTest = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj < 3)) or (
                           (obj1.flag[btap] == 1) and (obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                           obj1.count_balco_to_lnj < 3))

        holdTest = ((obj1.flag[btap] == 1) and (obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip) and (
                obj1.count_balco_to_lnj >= 3)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip) and (
                                   obj1.count_balco_to_lnj >= 3))

        kslkTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj >= 3)) or (
                obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip)) and (
                            obj1.count_kslk_supply_balco <= obj1.kslk_to_jsg_trip)) or (
                           (obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                           (obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) or (
                           (obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                           obj1.count_balco_to_lnj >= 3)) and (
                                   tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                   obj1.count_kslk_supply_balco <= obj1.kslk_to_jsg_trip)))

        holdAndBreakTest = ((obj1.flag[btap] == 1) and (
                ((obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (obj1.count_balco_to_lnj >= 3)) or (
                obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip)) and (
                                    obj1.count_kslk_supply_balco >= obj1.kslk_to_jsg_trip)) or (
                                   (obj1.flag[btap] == 0) and (
                                   total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and ((
                                                                                                     obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) or (
                                                                                                     (
                                                                                                             obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                                                                                                             obj1.count_balco_to_lnj >= 3)) and (
                                                                                                     tat_for_kslk_trip_and_txr * 24 <= total_hours + 120) and (
                                                                                                     obj1.count_kslk_supply_balco >= obj1.kslk_to_jsg_trip)))

        holdTillTxr = ((obj1.flag[btap] == 0) and (total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                (obj1.count_lanj_supply_balco >= obj1.lnj_to_jsg_trip) and (
                tat_for_kslk_trip_and_txr * 24 <= total_hours + 120)) or ((obj1.flag[btap] == 0) and (
                total_hours + 120 >= tat_for_lanj_trip_and_txr * 24) and (
                                                                                  obj1.count_lanj_supply_balco < obj1.lnj_to_jsg_trip) and (
                                                                                  obj1.count_balco_to_lnj >= 3) and (
                                                                                  tat_for_kslk_trip_and_txr * 24 >= total_hours + 120)))

        if kslkMaintanenceTest:
            # Going to kslk for maintenance
            balco_dep_date = balco_arrival_date + timedelta(hours=obj1.tat_dict['BALCO-Plant'] * 24)
            getGlobalList.balco_dep.append([btap_id,balco_dep_date,'KSLK-TXR'])
            tat_balco_kslk = obj1.tat_dict['BALCO-KSLK']
            balco_to_kslk_maintain(btap_id,balco_dep_date,tat_balco_kslk,obj1.flag,date)

        elif lanjTest:
            getGlobalList.balco_dep.append([btap_id,balco_dep_date,'LANJ'])
            tat_balco_lnj = obj1.tat_dict['BALCO-LNJ']
            balco_to_lanj(btap_id,balco_dep_date,tat_balco_lnj,obj1.flag,date)

        elif holdTest:
            # Hold at Balco
            pass

        elif kslkTest:
            # Going to kslk for Loading
            getGlobalList.balco_dep.append([btap_id,balco_dep_date,'KSLK'])
            tat_balco_kslk = obj1.tat_dict['BALCO-KSLK']
            balco_to_kslk(btap_id,balco_dep_date,tat_balco_kslk,obj1.flag,date)

        elif holdTest:
            # Hold at JSG
            for val in obj1.ls_balco_to_lnj_btap:
                if val[1] >= balco_dep_date - timedelta(hours=1.02 * 24) and (
                        val[1] - timedelta(hours=1.02 * 24)).month == balco_dep_date.month and val[1] <= balco_dep_date:
                    delayTime = val[1] - balco_dep_date + timedelta(hours=24)
                    delay = delayTime.day * 24 + delayTime.hour
                    btapDelaySortedDict(btap_id,delay)
                    obj1.holdlist.append(
                        [val[0],balco_arrival_date,balco_arrival_date + timedelta(hours=delay),'BALCO',obj1.sys_date])

        elif holdAndBreakTest:
            # Hold and break
            obj1.hold_list.append([btap_id,obj1.sys_date])

        elif holdTillTxr:
            # Hold till TXR
            balco_dep_date = balco_arrival_date + timedelta(
                days=obj1.btap_txr_details[btap_id].day - obj1.tat_dict['BALCO-KSLK'])
            getGlobalList.balco_dep.append([btap_id,balco_dep_date,'KSLK-TXR'])
            tat_balco_vizag = obj1.tat_dict['BALCO-KSLK']
            balco_to_kslk_maintain(btap_id,balco_dep_date,tat_balco_vizag,obj1.flag,date)


def schedule(date,specialSchedule=None,specialSchedule2=None,delayDate=None):
    global obj1,getGlobalList
    for btap_name,btap_info_list in obj1.sortedDict:
        if btap_name not in getGlobalList.btap_in_balco:
            destination = btap_info_list[0]
            time_to_travel = btap_info_list[1]
            load_status = btap_info_list[2]
            print("@" * 15,"Config of BTAP ",btap_name,"is ",load_status,destination,time_to_travel,"@" * 15)

            if obj1.flag2 == 0:
                if destination == "LNJ":
                    jsg_dep_date = obj1.sys_date - timedelta(hours=(obj1.tat_dict['JSG-LNJ'] * 24) - time_to_travel)
                    if obj1.sys_date.month == jsg_dep_date.month:
                        obj1.ls_jsg_to_lnj_btap.append([btap_name,jsg_dep_date,'LNJ'])
                obj1.ls_jsg_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_jsg_to_lnj_btap])]
                if btap_info_list[1] > 24:
                    continue
            time_spend = 24

            if specialSchedule:
                time_spend = round((delayDate.hour + (delayDate.minute // 60)),2)
            if specialSchedule2:
                time_spend = 24 - round((delayDate.hour + (delayDate.minute // 60)),2)

            if (specialSchedule or specialSchedule2) and obj1.sys_date + timedelta(hours=time_to_travel) > date:
                continue

            if destination == 'JSG':
                if obj1.sys_date > date:
                    continue
                jsg_arrival_date = obj1.sys_date + timedelta(hours=time_to_travel)
                x = check(jsg_arrival_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue
                if time_to_travel == 0 and load_status == 'E':
                    getGlobalList.jsg_dep.append([btap_name,obj1.sys_date,'LNJ'])
                else:
                    jsg_arrival_date = obj1.sys_date + timedelta(hours=time_to_travel)
                    getGlobalList.jsg_arrival.append([btap_name,jsg_arrival_date])
                    jsg_check(btap_name,jsg_arrival_date,obj1.flag,date)

            elif destination == 'Vizag':
                vizag_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)
                if obj1.sys_date > date:
                    continue
                x = check(vizag_ar_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue

                getGlobalList.vizag_arrival.append([btap_name,vizag_ar_date])
                vizag_dp_date = vizag_ar_date + timedelta(hours=obj1.tat_dict['TXR'] * 24)
                x = check(vizag_dp_date,date)

                if obj1.flag2 != 0 and x == 0:
                    continue
                getGlobalList.vizag_dep.append([btap_name,vizag_dp_date,'KSLK'])
                vizag_to_kslk(btap_name,vizag_dp_date,obj1.tat_dict['Vizag-KSLK'],obj1.flag,date)

            elif destination == 'LNJ':
                lanj_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)
                if obj1.sys_date > date:
                    continue

                x = check(lanj_ar_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue
                if time_to_travel == 0 and load_status == 'L':
                    getGlobalList.lnj_dep.append([btap_name,obj1.sys_date,'LNJ'])
                else:
                    getGlobalList.lnj_arrival.append([btap_name,lanj_ar_date])
                    lanj_dp_date = lanj_ar_date + timedelta(hours=obj1.tat_dict['LNJ-Plant'] * 24)
                    getGlobalList.lnj_dep.append([btap_name,lanj_dp_date,'JSG'])
                    lanj_to_jsg(btap_name,lanj_dp_date,obj1.tat_dict['LNJ-JSG'],obj1.flag,date)

            elif load_status == 'H':
                kslk_maint_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)
                if time_to_travel < 24:
                    getGlobalList.kslk_maintenance_arrival.append([btap_name,kslk_maint_ar_date])

            elif destination == 'KSLK-TXR':
                kslk_maint_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)
                if obj1.sys_date > date:
                    continue
                x = check(kslk_maint_ar_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue
                if time_to_travel < time_spend:
                    getGlobalList.kslk_maintenance_arrival.append([btap_name,kslk_maint_ar_date])

            elif destination == 'TXR':
                if (specialSchedule or specialSchedule2) and obj1.sys_date + timedelta(hours=time_to_travel) > date:
                    continue
                if obj1.sys_date > date:
                    continue
                txr_btap = int(btap_name[4:]) - 1
                if btap_name in getGlobalList.btap_in_vizag:
                    obj1.flag[txr_btap] = 1
                    getGlobalList.vizag_dep.append([btap_name,obj1.sys_date + timedelta(hours=time_to_travel),'KSLK'])
                    vizag_to_kslk(btap_name,obj1.sys_date + timedelta(hours=time_to_travel),
                                  obj1.tat_dict['Vizag-KSLK'],
                                  obj1.flag,date)
                else:
                    btap = int(btap_name[4:]) - 1
                    obj1.flag[btap] = 1
                    getGlobalList.kslk_dep.append(
                        [btap_name,
                         obj1.sys_date + timedelta(hours=time_to_travel + (obj1.tat_dict['KSLK-Plant'] * 24)),
                         'JSG'])
                    kslk_to_jsg(btap_name,
                                obj1.sys_date + timedelta(hours=time_to_travel + obj1.tat_dict['KSLK-Plant']),
                                obj1.tat_dict['KSLK-JSG'],obj1.flag,date)

            elif destination == 'KSLK':
                kslk_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)
                if obj1.sys_date > date:
                    continue
                x = check(kslk_ar_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue
                getGlobalList.kslk_arrival.append([btap_name,kslk_ar_date])
                day_difference = obj1.btap_txr_details[btap_name] - kslk_ar_date
                diff_days = day_difference.days * 24
                diff_hours = day_difference.seconds // 3600
                total_hours = diff_days + diff_hours + 5 * 24  # Adding Grace Period

                if btap_name in getGlobalList.btap_in_vizag:
                    if obj1.sys_date > date:
                        continue
                    kslk_dep_date = kslk_ar_date + timedelta(hours=obj1.tat_dict['KSLK-Plant'] * 24)
                    getGlobalList.kslk_dep.append([btap_name,kslk_dep_date,'JSG'])
                    tat_kslk_jsg = obj1.tat_dict['KSLK-JSG']
                    kslk_to_jsg(btap_name,kslk_dep_date,tat_kslk_jsg,obj1.flag,date)
                else:
                    d1 = total_hours
                    d2 = obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict['KSLK-JSG'] + obj1.tat_dict['JSG-Plant'] + \
                         obj1.tat_dict['JSG-KSLK']

                    if obj1.sys_date > date:
                        continue

                    if d1 + 120 > d2 * 24:
                        # Go to JSG
                        kslk_dep_date = kslk_ar_date + timedelta(hours=obj1.tat_dict['KSLK-Plant'] * 24)
                        if load_status == 'L' and time_to_travel == 0:
                            kslk_dep_date = kslk_ar_date

                        getGlobalList.kslk_dep.append([btap_name,kslk_dep_date,'JSG'])
                        tat_kslk_jsg = obj1.tat_dict['KSLK-JSG']
                        kslk_to_jsg(btap_name,kslk_dep_date,tat_kslk_jsg,obj1.flag,date)
                    else:
                        if d1 > 0:
                            # delay
                            btapDelaySortedDict(btap_name,d1 + time_to_travel)
                            obj1.holdlist.append(
                                [btap_name,kslk_ar_date,kslk_ar_date + timedelta(hours=d1 + time_to_travel),'KSLK',
                                 obj1.sys_date])

                        else:
                            # TXR
                            btap = int(btap_name[4:]) - 1
                            obj1.flag[btap] = 1
                            kslk_dep_date = kslk_ar_date + timedelta(hours=obj1.tat_dict['TXR'] * 24)
                            getGlobalList.kslk_dep.append([btap_name,kslk_dep_date,'JSG'])
                            tat_kslk_jsg = obj1.tat_dict['KSLK-JSG']
                            kslk_to_jsg(btap_name,kslk_dep_date,tat_kslk_jsg,obj1.flag,date)

            elif destination == "Exit":
                sys.exit(0)


def btapDelaySortedDict(btap,delay):
    global obj1,getGlobalList
    for val in obj1.sortedDict:
        if val[0] == btap:
            val[1][1] = val[1][1] + delay


def schedule_balco(date,specialSchedule=None,specialSchedule2=None,delayDate=None):
    global obj1,getGlobalList
    for btap_name,btap_info_list in obj1.sortedDict:
        if btap_name in getGlobalList.btap_in_balco:
            destination = btap_info_list[0]
            time_to_travel = btap_info_list[1]
            load_status = btap_info_list[2]
            print("@" * 15,"Config of BTAP ",btap_name,"is ",load_status,destination,time_to_travel,"@" * 15)
            if obj1.flag2 == 0:
                if destination == "LNJ":
                    balco_dep_date = obj1.sys_date - timedelta(hours=obj1.tat_dict['BALCO-LNJ'] * 24 - time_to_travel)
                    if obj1.sys_date.month == balco_dep_date.month:
                        obj1.ls_balco_to_lnj_btap.append([btap_name,balco_dep_date,'LNJ'])
                obj1.ls_balco_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_balco_to_lnj_btap])]
                if btap_info_list[1] > 24:
                    continue

            time_spend = 24

            if specialSchedule:
                time_spend = round((delayDate.hour + (delayDate.minute // 60)),2)
            if specialSchedule2:
                time_spend = 24 - round((delayDate.hour + (delayDate.minute // 60)),2)

            if (specialSchedule or specialSchedule2) and obj1.sys_date + timedelta(hours=time_to_travel) > date:
                continue

            if destination == 'BALCO':
                if obj1.sys_date > date:
                    continue
                balco_arrival_date = obj1.sys_date + timedelta(hours=time_to_travel)
                x = check(balco_arrival_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue
                if time_to_travel == 0 and load_status == 'L':
                    getGlobalList.lnj_dep.append([btap_name,obj1.sys_date,'LNJ'])
                else:
                    balco_arrival_date = obj1.sys_date + timedelta(hours=time_to_travel)
                    getGlobalList.balco_arrival.append([btap_name,balco_arrival_date])
                    balco_check(btap_name,balco_arrival_date,obj1.flag,date)

            elif destination == 'Vizag':
                vizag_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)
                if obj1.sys_date > date:
                    continue
                x = check(vizag_ar_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue

                getGlobalList.vizag_arrival.append([btap_name,vizag_ar_date])
                vizag_dp_date = vizag_ar_date + timedelta(hours=obj1.tat_dict['TXR'] * 24)
                x = check(vizag_dp_date,date)

                if obj1.flag2 != 0 and x == 0:
                    continue
                getGlobalList.vizag_dep.append([btap_name,vizag_dp_date,'KSLK'])
                vizag_to_kslk(btap_name,vizag_dp_date,obj1.tat_dict['Vizag-KSLK'],obj1.flag,date)

            elif destination == 'LNJ':
                lanj_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)
                if obj1.sys_date > date:
                    continue

                x = check(lanj_ar_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue
                if time_to_travel == 0 and load_status == 'L':
                    getGlobalList.lnj_dep.append([btap_name,obj1.sys_date,'LNJ'])
                else:
                    getGlobalList.lnj_arrival.append([btap_name,lanj_ar_date])
                    lanj_dp_date = lanj_ar_date + timedelta(hours=obj1.tat_dict['LNJ-Plant'] * 24)

                    getGlobalList.lnj_dep.append([btap_name,lanj_dp_date,'BALCO'])
                    lanj_to_balco(btap_name,lanj_dp_date,obj1.tat_dict['LNJ-BALCO'],obj1.flag,date)

            elif load_status == 'H':
                kslk_maint_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)

                if time_to_travel < 24:
                    getGlobalList.kslk_maintenance_arrival.append([btap_name,kslk_maint_ar_date])

            elif destination == 'KSLK-TXR':
                kslk_maint_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)

                if obj1.sys_date > date:
                    continue
                x = check(kslk_maint_ar_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue
                if time_to_travel < time_spend:
                    getGlobalList.kslk_maintenance_arrival.append([btap_name,kslk_maint_ar_date])

            elif destination == 'TXR':
                if (specialSchedule or specialSchedule2) and obj1.sys_date + timedelta(hours=time_to_travel) > date:
                    continue
                if obj1.sys_date > date:
                    continue
                txr_btap = int(btap_name[4:]) - 1
                if btap_name in getGlobalList.btap_in_vizag:
                    obj1.flag[txr_btap] = 1
                    getGlobalList.vizag_dep.append([btap_name,obj1.sys_date + timedelta(hours=time_to_travel),'KSLK'])
                    vizag_to_kslk(btap_name,obj1.sys_date + timedelta(hours=time_to_travel),
                                  obj1.tat_dict['Vizag-KSLK'],
                                  obj1.flag,date)
                else:
                    btap = int(btap_name[4:]) - 1
                    obj1.flag[btap] = 1
                    getGlobalList.kslk_dep.append(
                        [btap_name,
                         obj1.sys_date + timedelta(hours=time_to_travel + (obj1.tat_dict['KSLK-Plant'] * 24)),
                         'BALCO'])
                    kslk_to_balco(btap_name,
                                  obj1.sys_date + timedelta(hours=time_to_travel + obj1.tat_dict['KSLK-Plant']),
                                  obj1.tat_dict['KSLK-BALCO'],obj1.flag,date)

            elif destination == 'KSLK':
                kslk_ar_date = obj1.sys_date + timedelta(hours=time_to_travel)
                if obj1.sys_date > date:
                    continue
                x = check(kslk_ar_date,date)
                if obj1.flag2 != 0 and x == 0:
                    continue

                getGlobalList.kslk_arrival.append([btap_name,kslk_ar_date])
                day_difference = obj1.btap_txr_details[btap_name] - kslk_ar_date
                diff_days = day_difference.days * 24
                diff_hours = day_difference.seconds // 3600
                total_hours = diff_days + diff_hours + 5 * 24  # Adding Grace Period

                if btap_name in getGlobalList.btap_in_vizag:
                    if obj1.sys_date > date:
                        continue

                    kslk_dep_date = kslk_ar_date + timedelta(hours=obj1.tat_dict['KSLK-Plant'] * 24)
                    getGlobalList.kslk_dep.append([btap_name,kslk_dep_date,'BALCO'])
                    tat_kslk_balco = obj1.tat_dict['KSLK-BALCO']
                    kslk_to_balco(btap_name,kslk_dep_date,tat_kslk_balco,obj1.flag,date)
                else:
                    d1 = total_hours
                    d2 = obj1.tat_dict['KSLK-Plant'] + obj1.tat_dict['KSLK-BALCO'] + obj1.tat_dict['BALCO-Plant'] + \
                         obj1.tat_dict['BALCO-KSLK']

                    if obj1.sys_date > date:
                        continue

                    if d1 + 120 > d2 * 24:
                        # Go to BALCO
                        if time_to_travel == 0 and load_status == 'L':
                            getGlobalList.kslk_dep.append([btap_name,obj1.sys_date,'BALCO'])
                        else:
                            kslk_dep_date = kslk_ar_date + timedelta(hours=obj1.tat_dict['KSLK-Plant'] * 24)
                            getGlobalList.kslk_dep.append([btap_name,kslk_dep_date,'BALCO'])
                            tat_kslk_balco = obj1.tat_dict['KSLK-BALCO']
                            kslk_to_balco(btap_name,kslk_dep_date,tat_kslk_balco,obj1.flag,date)

                    else:
                        if d1 > 0:
                            # delay
                            btapDelaySortedDict(btap_name,d1 + time_to_travel)
                            obj1.holdlist.append(
                                [btap_name,kslk_ar_date,kslk_ar_date + timedelta(hours=d1 + time_to_travel),'KSLK',
                                 obj1.sys_date])


                        else:
                            # TXR
                            btap = int(btap_name[4:]) - 1
                            obj1.flag[btap] = 1
                            kslk_dep_date = kslk_ar_date + timedelta(hours=obj1.tat_dict['TXR'] * 24)
                            getGlobalList.kslk_dep.append([btap_name,kslk_dep_date,'BALCO'])
                            tat_kslk_balco = obj1.tat_dict['KSLK-BALCO']
                            kslk_to_balco(btap_name,kslk_dep_date,tat_kslk_balco,obj1.flag,date)

            elif destination == "Exit":
                sys.exit(0)


def loadingUnloadingCheckJSG(date,delayDate=None):
    global obj1,getGlobalList
    ans = []
    f = 0
    temp_jsg_arrival = sorted(getGlobalList.jsg_arrival,key=lambda x: x[1])
    for val in temp_jsg_arrival:
        plant1_test = obj1.plant1_demand > 0 and len(obj1.plant1_arrival) == 0
        plant2_test = obj1.plant1_demand > 0 and len(obj1.plant1_arrival) >= 1 and obj1.plant2_demand > 0 and len(
            obj1.plant2_arrival) < 2

        compare_plant1_plant2 = obj1.plant1_demand > 0 and len(
            obj1.plant1_arrival) >= 1 and obj1.plant2_demand > 0 and len(obj1.plant2_arrival) >= 2

        plant1_delay_test = obj1.plant1_demand > 0 and len(obj1.plant1_arrival) >= 1 and obj1.plant2_demand == 0
        plant2_delay_test = obj1.plant1_demand == 0 and obj1.plant2_demand > 0
        over_test = obj1.plant1_demand == 0 and obj1.plant2_demand == 0

        p1_delay = 0
        p2_delay = 0
        if len(obj1.plant1_arrival) > 0:
            p1_delay1 = obj1.plant1_arrival[-1][-1] + timedelta(hours=14)
            p1_delay = (p1_delay1.day * 24 + p1_delay1.hour) - (val[1].day * 24 + val[1].hour)
        if len(obj1.plant2_arrival) > 0:
            p2_delay1 = obj1.plant2_arrival[0][-1] + timedelta(hours=10.5)
            p2_delay = (p2_delay1.day * 24 + p2_delay1.hour) - (val[1].day * 24 + val[1].hour)

        if plant1_test:
            # Going to Plant 1
            obj1.plant1_arrival.append([val[0],val[1]])
            obj1.plant1_demand -= 1
        elif plant2_test:
            # Going to Plant 2
            obj1.plant2_arrival.append([val[0],val[1]])
            obj1.plant2_demand -= 1
        elif compare_plant1_plant2:
            if p1_delay <= 0:
                obj1.plant1_arrival.append([val[0],val[1]])
                obj1.plant1_demand -= 1
            elif p2_delay <= 0:
                obj1.plant2_arrival.append([val[0],val[1]])
                obj1.plant2_demand -= 1
            # Compare Delay of Plant 1 and Plant 2
            elif p1_delay <= p2_delay:
                # Delay in P1
                f = 1
                obj1.plant1_arrival.append([val[0],val[1] + timedelta(hours=p1_delay)])
                obj1.plant1_demand -= 1
                ans.append([val[0],p1_delay])
            else:
                # Delay in P2
                f = 1
                obj1.plant2_arrival.append([val[0],val[1] + timedelta(hours=p2_delay)])
                obj1.plant2_demand -= 1
                ans.append([val[0],p2_delay])
        elif plant1_delay_test:
            obj1.plant1_arrival.append([val[0],val[1]])
            obj1.plant1_demand -= 1
            if p1_delay > 0:
                # Delay in plant 1
                ans.append([val[0],p1_delay])
                f = 1
        elif plant2_delay_test:
            obj1.plant2_arrival.append([val[0],val[1]])
            obj1.plant2_demand -= 1
            if p2_delay > 0:
                # Delay in plant 2
                ans.append([val[0],p2_delay])
                f = 1
        elif over_test:
            # Stop the BTAP schedule
            pass
    if f == 1:
        ans.append(date)
    return ans


def loadingUnloadingCheckJSG_delay():
    global obj1,getGlobalList1
    temp_jsg_arrival = sorted(getGlobalList.jsg_arrival,key=lambda x: x[1])
    for val in temp_jsg_arrival:
        plant1_test = obj1.plant1_demand > 0 and len(obj1.plant1_arrival) == 0
        plant2_test = obj1.plant1_demand > 0 and len(obj1.plant1_arrival) >= 1 and obj1.plant2_demand > 0 and len(
            obj1.plant2_arrival) < 2

        compare_plant1_plant2 = obj1.plant1_demand > 0 and len(
            obj1.plant1_arrival) >= 1 and obj1.plant2_demand > 0 and len(obj1.plant2_arrival) >= 2

        plant1_delay_test = obj1.plant1_demand > 0 and len(obj1.plant1_arrival) >= 1 and obj1.plant2_demand == 0
        plant2_delay_test = obj1.plant1_demand == 0 and obj1.plant2_demand > 0
        over_test = obj1.plant1_demand == 0 and obj1.plant2_demand == 0

        p1_delay = 0
        p2_delay = 0
        if len(obj1.plant1_arrival) > 0:
            p1_delay1 = obj1.plant1_arrival[-1][-1] + timedelta(hours=14)
            p1_delay = (p1_delay1.day * 24 + p1_delay1.hour) - (val[1].day * 24 + val[1].hour)
        if len(obj1.plant2_arrival) > 0:
            p2_delay1 = obj1.plant2_arrival[0][-1] + timedelta(hours=10.5)
            p2_delay = (p2_delay1.day * 24 + p2_delay1.hour) - (val[1].day * 24 + val[1].hour)

        if plant1_test:
            # Going to Plant 1
            obj1.plant1_arrival.append([val[0],val[1]])
            obj1.plant1_demand -= 1
        elif plant2_test:
            # Going to Plant 2
            obj1.plant2_arrival.append([val[0],val[1]])
            obj1.plant2_demand -= 1
        elif compare_plant1_plant2:
            if p1_delay <= 0:
                obj1.plant1_arrival.append([val[0],val[1]])
                obj1.plant1_demand -= 1
            elif p2_delay <= 0:
                obj1.plant2_arrival.append([val[0],val[1]])
                obj1.plant2_demand -= 1
            # Compare Delay of Plant 1 and Plant 2
            elif p1_delay <= p2_delay:
                # Delay in P1
                obj1.plant1_arrival.append([val[0],val[1] + timedelta(hours=p1_delay)])
                obj1.plant1_demand -= 1
            else:
                # Delay in P2
                obj1.plant2_arrival.append([val[0],val[1] + timedelta(hours=p2_delay)])
                obj1.plant2_demand -= 1
        elif plant1_delay_test:
            obj1.plant1_arrival.append([val[0],val[1]])
            obj1.plant1_demand -= 1
        elif plant2_delay_test:
            obj1.plant2_arrival.append([val[0],val[1]])
            obj1.plant2_demand -= 1
        elif over_test:
            # Stop the BTAP schedule
            pass


def loadingUnloadingCheckbalco(date,delayDate=None):
    global obj1,getGlobalList
    ans = []
    if len(getGlobalList.balco_arrival) > 1:
        temp = []
        tempved = []
        for val in getGlobalList.balco_arrival:
            temp.append(val[1])
            tempved.append(val[0])
        if len(temp) > 0:
            mini_time = min(temp)
            maxi_time = max(temp)

            ls = []
            temp.clear()
            for k in getGlobalList.hist_kslk_dep:
                if k[1] >= mini_time - timedelta(days=obj1.tat_dict['KSLK-BALCO']) and k[1] <= maxi_time - timedelta(
                        days=obj1.tat_dict['KSLK-BALCO']) and k[
                    0] in tempved and \
                        (not delayDate or (
                                delayDate and k[1] + timedelta(days=obj1.tat_dict['KSLK-BALCO']) >= delayDate)):
                    ls.append(k)
                    temp.append(k[1])

            if len(temp) > 0:
                for val in ls:
                    delay = 24 - val[1].hour
                    ans.append([val[0],delay])
                    obj1.holdlist.append([val[0],val[1],val[1] + timedelta(hours=delay),'BALCO',obj1.sys_date])
                ans.append(date)
                return ans
            else:

                mini_balco_arrival = min(getGlobalList.balco_arrival,key=lambda x: x[1])[1]
                maxi_balco_arrival = max(getGlobalList.balco_arrival,key=lambda x: x[1])[1]

                temp.clear()

                for k in getGlobalList.hist_lnj_dep:
                    if k[1] >= mini_balco_arrival - timedelta(days=obj1.tat_dict['LNJ-BALCO']) and k[0] in tempved and \
                            k[
                                1] <= maxi_balco_arrival - timedelta(
                        days=obj1.tat_dict['LNJ-BALCO']):
                        ls.append(k)
                        temp.append(k[1])

                if len(temp) > 0:
                    for val in ls:
                        delay = 24 - val[1].hour
                        ans.append([val[0],delay])
                        obj1.holdlist.append([val[0],val[1],val[1] + timedelta(hours=delay),'BALCO',obj1.sys_date])
                    ans.append(date)
                    return ans
    return ans


def loadingUnloadingCheckLNJ(date):
    global obj1,getGlobalList
    if len(getGlobalList.lnj_arrival) > 2:
        temp = []
        for val in getGlobalList.lnj_arrival:
            temp.append(val[1])
        if len(temp) > 0:
            miniTime = min(temp)
            maxiTime = max(temp)
            mini = getGlobalList.lnj_arrival[0]
            maxi = getGlobalList.lnj_arrival[0]
            for val in getGlobalList.lnj_arrival:
                if val[1] == miniTime:
                    mini = val
                elif val[1] == maxiTime:
                    maxi = val
            delay = mini[1].hour + 24 - maxi[1].hour
            ans = [[maxi[0],delay]]
            obj1.holdlist.append([maxi[0],maxi[1],maxi[1] + timedelta(hours=delay),'LNJ',obj1.sys_date])
            ans.append(date)
            return ans
    return []


def updateLnjSortedDict():
    global obj1,getGlobalList

    val = []
    for btap in obj1.sortedDict:
        if btap[1][0] == "LNJ":
            val.append(btap)
    if len(val) > 0:
        x = sorted(val,key=lambda t: t[1][1])
        temp = x[0]
        for i in range(1,len(x)):
            if x[i][1][1] < (temp[1][1] + 8):
                obj1.holdlist.append([x[i][0],obj1.sys_date + timedelta(hours=x[i][1][1]),
                                      obj1.sys_date + timedelta(hours=temp[1][1] + 8),'LNJ',obj1.sys_date])
                x[i][1][1] = temp[1][1] + 8
            temp = x[i]

        for i in range(len(obj1.sortedDict)):
            for j in range(0,len(x)):
                if obj1.sortedDict[i][0] == x[j][0]:
                    obj1.sortedDict[i][1] = x[j][1]


def loadingUnloadingCheck(date,delayDate=None):
    global obj1,getGlobalList
    ans1 = []
    ans2 = []
    ans3 = []
    if (delayDate and date.day > delayDate.day) or not delayDate:
        ans1.extend(loadingUnloadingCheckJSG(date,delayDate))
        ans3.extend(loadingUnloadingCheckbalco(date,delayDate))
    ans2.extend(loadingUnloadingCheckLNJ(date))
    return ans1,ans2,ans3


def removeSortedDict(sdate):
    global obj1,getGlobalList
    resultTempTable = obj1.resultTable[:sdate]
    obj1.resultTable.clear()
    obj1.resultTable = resultTempTable[:]
    obj1.sys_date = datetime.strptime("{0}-{1}-{2}".format(sdate,obj1.month,obj1.year),'%d-%m-%Y')


def removeHistoryLnjData(timeStamp):
    global obj1,getGlobalList
    timeStamp = timeStamp.day
    obj1.count_kslk_supply_jsg = getGlobalList.hist_count_kslk_supply_jsg[-1]
    obj1.count_lanj_supply_jsg = getGlobalList.hist_count_lnj_supply_jsg[-1]
    obj1.count_kslk_supply_balco = getGlobalList.hist_count_kslk_supply_balco[-1]
    obj1.count_lanj_supply_balco = getGlobalList.hist_count_lnj_supply_balco[-1]
    obj1.count_balcoDep_not_lnjArr = getGlobalList.hist_count_balcoDep_not_lnjArr[-1]
    obj1.count_jsgDep_not_lnjArr = getGlobalList.hist_count_jsgDep_not_lnjArr[-1]
    obj1.plant1_demand = getGlobalList.hist_plant1_demand[-1]
    obj1.plant2_demand = getGlobalList.hist_plant2_demand[-1]
    obj1.plantdates = getGlobalList.hist_plant_record[-1][:]
    obj1.flag = getGlobalList.hist_flag[-1]
    ls = []
    for j in obj1.ls_jsg_to_lnj_btap:
        if j[1].day - obj1.tat_dict['JSG-Plant'] < timeStamp:
            ls.append(j)
    obj1.ls_jsg_to_lnj_btap.clear()
    obj1.ls_jsg_to_lnj_btap = ls[:]
    obj1.ls_jsg_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_jsg_to_lnj_btap])]

    ls.clear()

    ls = []
    for j in obj1.ls_balco_to_lnj_btap:
        if j[1].day - obj1.tat_dict['BALCO-Plant'] < timeStamp:
            ls.append(j)
    obj1.ls_balco_to_lnj_btap.clear()
    obj1.ls_balco_to_lnj_btap = ls[:]
    obj1.ls_balco_to_lnj_btap = [list(i) for i in set([tuple(i) for i in obj1.ls_balco_to_lnj_btap])]

    ls.clear()


def sortThesorteddict():
    global obj1,getGlobalList
    t1 = obj1.sortedDict
    for i in range(0,len(t1)):
        if t1[i][1][0] == "JSG":
            t1[i][1][1] = t1[i][1][1] + (
                    obj1.tat_dict["JSG-Plant"] + obj1.tat_dict["JSG-KSLK"] + obj1.tat_dict["KSLK-Plant"] +
                    obj1.tat_dict["KSLK-JSG"]) * 24

        if t1[i][1][0] == "KSLK":
            t1[i][1][1] = t1[i][1][1] + (
                    obj1.tat_dict["KSLK-Plant"] + obj1.tat_dict["KSLK-JSG"] + obj1.tat_dict["JSG-Plant"] +
                    obj1.tat_dict["JSG-KSLK"]) * 24

        if t1[i][1][0] == "LNJ":
            t1[i][1][1] = t1[i][1][1] + (
                    obj1.tat_dict["LNJ-Plant"] + obj1.tat_dict["JSG-LNJ"] + obj1.tat_dict["JSG-Plant"] +
                    obj1.tat_dict["LNJ-JSG"]) * 24

    ti = sorted(t1,key=lambda x: x[1][1])

    for i in range(0,len(ti)):
        if ti[i][1][0] == "JSG":
            ti[i][1][1] = round(ti[i][1][1] - (
                    obj1.tat_dict["JSG-Plant"] + obj1.tat_dict["JSG-KSLK"] + obj1.tat_dict["KSLK-Plant"] +
                    obj1.tat_dict["KSLK-JSG"]) * 24,2)

        if ti[i][1][0] == "KSLK":
            ti[i][1][1] = round(ti[i][1][1] - (
                    obj1.tat_dict["KSLK-Plant"] + obj1.tat_dict["KSLK-JSG"] + obj1.tat_dict["JSG-Plant"] +
                    obj1.tat_dict["JSG-KSLK"]) * 24,2)

        if ti[i][1][0] == "LNJ":
            ti[i][1][1] = round(ti[i][1][1] - (
                    obj1.tat_dict["LNJ-Plant"] + obj1.tat_dict["JSG-LNJ"] + obj1.tat_dict["JSG-Plant"] +
                    obj1.tat_dict["LNJ-JSG"]) * 24,2)

    ti.reverse()
    obj1.sortedDict = ti
    return obj1.sortedDict


def updateKslkSortedDict():
    global obj1,getGlobalList
    tempdict = sorted(obj1.sortedDict,key=lambda x: x[1][1])
    templs = []

    for ls in tempdict:
        if (ls[1][0] == 'KSLK' and ls[1][1] <= 24):
            templs.append(ls)
    dic = {}
    if len(templs) > 1:
        delay = 0
        for ind,val in enumerate(templs[1:],1):
            rem = templs[ind - 1][1][1] + 12
            arrivalTime = val[1][1]
            if rem > arrivalTime:
                delay += rem - arrivalTime
                obj1.holdlist.append([val[0],obj1.sys_date + timedelta(hours=arrivalTime),
                                      obj1.sys_date + timedelta(hours=arrivalTime + delay),'KSLK',obj1.sys_date])
            newval = val[1][1] + delay
            if val[0] not in getGlobalList.btap_in_vizag and newval >= 24:
                btapindex = int(val[0][4:]) - 1
            templs[ind][1][1] += delay
            dic[val[0]] = val[1][1]
    for ind,val in enumerate(obj1.sortedDict):
        if val[0] in dic:
            obj1.sortedDict[ind][1][1] = dic[val[0]]


def delaySortedDict(delayls,delayFirstTime=None):
    global obj1,getGlobalList
    delayDic = {}
    if delayFirstTime:
        for val in delayls:
            delayDic[val[0]] = val[1]
    else:
        for val in delayls[:-1]:
            delayDic[val[0]] = val[1]
    for ind,val in enumerate(obj1.sortedDict):
        btap_name,btap_info_list = val
        if btap_name in delayDic:
            obj1.sortedDict[ind][1][1] = btap_info_list[1] + delayDic[btap_name]


def plantEntry(specialSchedule=None,specialSchedule2=None,date=None,specialSchedule3=None,sdate=None):
    global obj1,getGlobalList
    if specialSchedule:
        obj1.timeSpend = round((date.hour + (date.minute // 60)),2)
    if specialSchedule2:
        obj1.timeSpend = 24 - round((date.hour + (date.minute // 60)),2)
    if specialSchedule3:
        obj1.timeSpend = round((date.hour + (date.minute // 60)),2) - round((sdate.hour + (sdate.minute // 60)),2)

    for val in obj1.sortedDict:
        if val[1][1] <= 24 - obj1.timeSpend:
            if val[1][0] == "BALCO":
                obj1.plantdates.append([val[0],obj1.sys_date + timedelta(hours=val[1][1]),
                                        obj1.sys_date + timedelta(
                                            hours=(val[1][1] + (obj1.tat_dict["BALCO-Plant"] * 24))),
                                        val[1][0]])

            if val[1][0] == "KSLK" or val[1][0] == "TXR":
                obj1.plantdates.append([val[0],obj1.sys_date + timedelta(hours=val[1][1]),
                                        obj1.sys_date + timedelta(
                                            hours=(val[1][1] + (obj1.tat_dict["KSLK-Plant"] * 24))),
                                        "KSLK-Plant"])
            if val[1][0] == "LNJ":
                obj1.plantdates.append([val[0],obj1.sys_date + timedelta(hours=val[1][1]),
                                        obj1.sys_date + timedelta(
                                            hours=(val[1][1] + (obj1.tat_dict["LNJ-Plant"] * 24))),
                                        val[1][0]])
    for val in obj1.plant1_arrival:
        obj1.plantdates.append([val[0],val[1],val[1] + timedelta(days=obj1.tat_dict['Plant1']),'JSG-P1'])
    for val in obj1.plant2_arrival:
        obj1.plantdates.append([val[0],val[1],val[1] + timedelta(days=obj1.tat_dict['Plant2']),'JSG-P2'])

    obj1.plantdates = [list(item) for item in set(tuple(row) for row in obj1.plantdates)]


def fun(sdate,endDate=None,ls=None,delayFirstTime=False,delayDate=None,specialSchedule2=None):
    global obj1,getGlobalList
    eDate = monthrange(sdate.year,sdate.month)[1] + 1
    if endDate:
        eDate = endDate.day
    for dat in range(sdate.day,eDate):
        date = datetime.strptime("{0}-{1}-{2} 23:59:59".format(dat,obj1.month,obj1.year),'%d-%m-%Y %H:%M:%S')
        print("obj1.sys_date is",obj1.sys_date)
        if delayFirstTime and sdate.day == dat:
            mnt = delayDate.month
            h = delayDate.hour
            m = delayDate.minute
            date = datetime.strptime("{0}-{1}-{4} {2}:{3}:59".format(dat,mnt,h,m,obj1.year),'%d-%m-%Y %H:%M:%S')
        elif sdate.day != dat:
            date = datetime.strptime("{0}-{1}-{2} 23:59:59".format(dat,obj1.month,obj1.year),'%d-%m-%Y %H:%M:%S')

        if not delayFirstTime:
            if ls != None and len(ls) > 0:
                clearList2()
                delaySortedDict(ls)
            else:
                clearList()
        else:
            delaySortedDict(ls,delayFirstTime)

        updateBalcoSortedDict()
        updateLnjSortedDict()
        updateKslkSortedDict()
        sortThesorteddict()
        goKSLK()
        goLNJ()
        if delayFirstTime and sdate.day == dat:
            date = datetime.strptime("{0}-{1}-{2} 23:59:59".format(dat,obj1.month,obj1.year),'%d-%m-%Y %H:%M:%S')
            if obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip or obj1.count_kslk_supply_jsg < obj1.kslk_to_jsg_trip:
                schedule(date,specialSchedule2=specialSchedule2,delayDate=sdate)
            if obj1.count_kslk_supply_balco < obj1.kslk_to_balco_trip or obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip:
                schedule_balco(date,specialSchedule2=specialSchedule2,delayDate=sdate)
        else:
            if obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip or obj1.count_kslk_supply_jsg < obj1.kslk_to_jsg_trip:
                schedule(date)
            if obj1.count_kslk_supply_balco < obj1.kslk_to_balco_trip or obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip:
                schedule_balco(date)

        obj1.flag2 = 1
        if endDate != None:
            pass
        elif not delayFirstTime:
            ls1,ls2,ls3 = loadingUnloadingCheck(date,delayDate)

            if len(ls1) > 0:
                sdate = ls1[-1]
                # print("JSG ISSUE", ls1)
                removeSortedDict(sdate.day)
                removeHistoryLnjData(sdate)
                return ls1
            elif len(ls3) > 0:
                sdate = ls3[-1]
                # print("Balco ISSUE", ls3)
                removeSortedDict(sdate.day)
                removeHistoryLnjData(sdate)
                return ls3
            elif len(ls2) > 0:
                # print("LNJ ISSUE", ls2, ls2[-1])
                sdate = ls2[-1]
                removeHistoryLnjData(sdate)
                return ls2
            else:
                ls = []
        if delayFirstTime:
            loadingUnloadingCheckJSG_delay()
        plantEntry()
        if delayFirstTime and sdate.day == dat:
            obj1.sortedDict = updateSortedDict(specialSchedule2=specialSchedule2,date=sdate)
        else:
            obj1.sortedDict = updateSortedDict()
        if endDate:
            endDate = None
            pass
        # pprint.pprint(obj1.sortedDict)
        obj1.resultTable.append(obj1.sortedDict)
        obj1.holdlist = [list(item) for item in set(tuple(row) for row in obj1.holdlist)]
        obj1.timeSpend = 0
        if delayFirstTime:
            ls.clear()
            day = obj1.sys_date.day + 1
            obj1.sys_date = datetime.strptime("{0}-{1}-{2}".format(day,obj1.month,obj1.year),'%d-%m-%Y')

        else:
            obj1.sys_date = obj1.sys_date + timedelta(days=1)
    return []


def convert_json(value):
    try:
        value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        pass
    return value


def process(value):
    if isinstance(value, str):
        return convert_json(value)
    elif isinstance(value, int):
        return convert_json(value)
    elif isinstance(value, float):
        return convert_json(value)
    elif isinstance(value, list):
        output = list()
        for i in value:
            if isinstance(i, list):
                output.append(process(i))
            else:
                output.append(convert_json(i))
        return output
    else:
        return convert_json(value)


def date_hook(json_dict):
    output = dict()
    for (key, value) in json_dict.items():
        output[key] = process(value)
    return output


def retrievingArgument(break1=None):
    global obj1, getGlobalList
    dic = {}
    if break1 is not None:
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 13 for SQL Server};SERVER=wfsdeve.database.windows.net;DATABASE=vedanta ACT;UID=wfsdev;PWD=Tata@123")
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        connection = engine.connect()
        data = connection.execute("SELECT json_data from dispatch_varibles_landing_table").fetchall()
        if data:
            dic = json.loads(data[0][0], object_hook=date_hook)
    else:
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 13 for SQL Server};SERVER=wfsdeve.database.windows.net;DATABASE=vedanta ACT;UID=wfsdev;PWD=Tata@123")
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        connection = engine.connect()
        data = connection.execute("SELECT json_data from dispatch_varibles_main_table").fetchall()
        if data:
            dic = json.loads(data[0][0], object_hook=date_hook)
    obj1.resultTable = dic['obj1.resultTable']

    obj1.ls_jsg_to_lnj_btap = dic['obj1.ls_jsg_to_lnj_btap']
    obj1.ls_balco_to_lnj_btap = dic['obj1.ls_balco_to_lnj_btap']
    getGlobalList.hist_jsg_arrival = dic['getGlobalList.hist_jsg_arrival']
    getGlobalList.hist_jsg_dep = dic['getGlobalList.hist_jsg_dep']
    getGlobalList.hist_lnj_arrival = dic['getGlobalList.hist_lnj_arrival']
    getGlobalList.hist_lnj_dep = dic['getGlobalList.hist_lnj_dep']
    getGlobalList.hist_vizag_arrival = dic['getGlobalList.hist_vizag_arrival']
    getGlobalList.hist_vizag_dep = dic['getGlobalList.hist_vizag_dep']
    getGlobalList.hist_kslk_arrival = dic['getGlobalList.hist_kslk_arrival']
    getGlobalList.hist_kslk_dep = dic['getGlobalList.hist_kslk_dep']
    getGlobalList.hist_jsg_circuit = dic['getGlobalList.hist_jsg_circuit']
    getGlobalList.hist_kslk_maintain = dic['getGlobalList.hist_kslk_maintain']
    getGlobalList.hist_count_lnj_supply_balco = dic['getGlobalList.hist_count_lnj_supply_balco']
    getGlobalList.hist_count_lnj_supply_jsg = dic['getGlobalList.hist_count_lnj_supply_jsg']
    getGlobalList.hist_count_kslk_supply_jsg = dic['getGlobalList.hist_count_kslk_supply_jsg']
    getGlobalList.hist_count_kslk_supply_balco = dic['getGlobalList.hist_count_kslk_supply_balco']
    getGlobalList.hist_balco_arrival = dic['getGlobalList.hist_balco_arrival']
    getGlobalList.hist_balco_dep = dic['getGlobalList.hist_balco_dep']
    getGlobalList.hist_flag = dic['getGlobalList.hist_flag']
    getGlobalList.hist_txr_flag = dic['getGlobalList.hist_txr_flag']
    getGlobalList.hist_count_balcoDep_not_lnjArr = dic['getGlobalList.hist_count_balcoDep_not_lnjArr']
    getGlobalList.hist_count_jsgDep_not_lnjArr = dic['getGlobalList.hist_count_jsgDep_not_lnjArr']
    getGlobalList.hist_plant1_demand = dic['getGlobalList.hist_plant1_demand']
    getGlobalList.hist_plant2_demand = dic['getGlobalList.hist_plant2_demand']
    getGlobalList.hist_plant_record = dic['getGlobalList.hist_plant_record']
    getGlobalList.btap_in_balco = dic['getGlobalList.btap_in_balco']
    getGlobalList.hist_hold_records = dic['getGlobalList.hist_hold_records']
    obj1.count_kslk_supply_jsg = dic['obj1.count_kslk_supply_jsg']
    obj1.count_kslk_supply_balco = dic['obj1.count_kslk_supply_balco']
    obj1.count_lanj_supply_jsg = dic['obj1.count_lanj_supply_jsg']
    obj1.count_lanj_supply_balco = dic['obj1.count_lanj_supply_balco']
    obj1.flag2 = dic['obj1.flag2']
    obj1.initial_cylon_capacity = dic['obj1.initial_cylon_capacity']
    obj1.vessel_arrival_date = dic['obj1.vessel_arrival_date']
    obj1.cylon_capacity = dic['obj1.cylon_capacity']
    obj1.lnj_jsg = dic['obj1.lnj_jsg']
    obj1.lnj_balco = dic['obj1.lnj_balco']
    obj1.kslk_jsg = dic['obj1.kslk_jsg']
    obj1.kslk_balco = dic['obj1.kslk_balco']
    obj1.demand_jsg = dic['obj1.demand_jsg']
    obj1.demand_balco = dic['obj1.demand_balco']
    obj1.plant1_demand = dic['obj1.plant1_demand']
    obj1.plant2_demand = dic['obj1.plant2_demand']
    obj1.month = dic['obj1.month']
    obj1.btap_capacity = dic['obj1.btap_capacity']
    obj1.btap_info = dic['obj1.btap_info']
    obj1.btap_txr_details = dic['obj1.btap_txr_details']
    obj1.year = dic['obj1.year']


def dispatchSchedulerRun(delay_choice=None):
    global obj1, getGlobalList

    if delay_choice != None and delay_choice.lower() == "n":
        obj1.sortedDict = split_dict()
        obj1.sortedDict = newPatternSortedDict()
        obj1.resultTable.append(obj1.sortedDict)

    sdate = obj1.sys_date
    if delay_choice.lower() == "b":
        # run load from mid day to end then continue loop
        goKSLK()
        goLNJ()
        # Calling schedule
        date = datetime.strptime("{0}-{1}-{2} 23:59:59".format(sdate.day,sdate.month,sdate.year),'%d-%m-%Y %H:%M:%S')
        if obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip or obj1.count_kslk_supply_jsg < obj1.kslk_to_jsg_trip:
            schedule(date,specialSchedule2=True,delayDate=sdate)
        if obj1.count_kslk_supply_balco < obj1.kslk_to_balco_trip or obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip:
            schedule_balco(date,specialSchedule2=True,delayDate=sdate)

        loadingUnloadingCheckJSG_delay()
        plantEntry(date=obj1.sys_date,specialSchedule2=True)

        obj1.sortedDict = updateSortedDict(specialSchedule2=True,date=obj1.sys_date)
        obj1.timeSpend = 0
        obj1.resultTable.append(obj1.sortedDict)

        # Take new date and run the load
        sdate = datetime.strptime("{0}-{1}-{2}".format(obj1.sys_date.day + 1,obj1.sys_date.month,obj1.year),'%d-%m-%Y')
        obj1.sys_date = sdate

    ls3 = []
    while True:
        ls3 = fun(sdate,ls=ls3)
        if len(ls3) > 0:
            sdate = ls3[-1]
        else:
            break


ls = ['obj1.resultTable',
      'obj1.ls_jsg_to_lnj_btap',
      'obj1.ls_balco_to_lnj_btap',
      'getGlobalList.hist_jsg_arrival',
      'getGlobalList.hist_jsg_dep',
      'getGlobalList.hist_lnj_arrival',
      'getGlobalList.hist_lnj_dep',
      'getGlobalList.hist_vizag_arrival',
      'getGlobalList.hist_vizag_dep',
      'getGlobalList.hist_kslk_arrival',
      'getGlobalList.hist_kslk_dep',
      'getGlobalList.hist_jsg_circuit',
      'getGlobalList.hist_kslk_maintain',
      'getGlobalList.hist_count_lnj_supply_jsg',
      'getGlobalList.hist_count_lnj_supply_balco',
      'getGlobalList.hist_count_kslk_supply_jsg',
      'getGlobalList.hist_count_kslk_supply_balco',
      'getGlobalList.hist_flag',
      'getGlobalList.hist_txr_flag',
      'getGlobalList.hist_store_txr',
      'getGlobalList.hist_balco_arrival',
      'getGlobalList.hist_balco_dep',
      'getGlobalList.hist_count_balcoDep_not_lnjArr',
      'getGlobalList.hist_count_jsgDep_not_lnjArr',
      'getGlobalList.hist_plant1_demand',
      'getGlobalList.hist_plant2_demand',
      'getGlobalList.hist_plant_record',
      'getGlobalList.btap_in_balco',
      'getGlobalList.hist_hold_records',
      'obj1.count_kslk_supply_jsg',
      'obj1.count_kslk_supply_balco',
      'obj1.count_lanj_supply_jsg',
      'obj1.count_lanj_supply_balco',
      'obj1.flag2',
      'obj1.initial_cylon_capacity',
      'obj1.vessel_arrival_date',
      'obj1.cylon_capacity',
      'obj1.lnj_jsg',
      'obj1.lnj_balco',
      'obj1.kslk_jsg',
      'obj1.kslk_balco',
      'obj1.demand_jsg',
      'obj1.demand_balco',
      'obj1.plant1_demand',
      'obj1.plant2_demand',
      'obj1.month',
      'obj1.btap_capacity',
      'obj1.btap_info',
      'obj1.btap_txr_details',
      'obj1.year'
      ]


def dispatchSchedulerRunDelayAfter(dateEnd,run_delay_choice,run_delay_json=None,sdate=None):
    global obj1,getGlobalList
    obj1.sys_date = datetime.strptime("{0}-{1}-{2}".format(dateEnd.day,obj1.month,obj1.year),'%d-%m-%Y')
    if sdate != None:
        obj1.sys_date = sdate
    else:
        obj1.sortedDict = obj1.resultTable[-1]

    dat = dateEnd.day
    h = dateEnd.hour
    m = dateEnd.minute
    date = datetime.strptime("{0}-{1}-{4} {1}:{2}:59".format(dat,obj1.month,h,m,obj1.year),'%d-%m-%Y %H:%M:%S')
    special_schedule = True
    clearList()

    updateBalcoSortedDict()
    updateLnjSortedDict()
    updateKslkSortedDict()
    sortThesorteddict()
    goKSLK()
    goLNJ()

    # Calling schedule
    if obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip or obj1.count_kslk_supply_jsg < obj1.kslk_to_jsg_trip:
        schedule(date,special_schedule,delayDate=date)

    if obj1.count_kslk_supply_balco < obj1.kslk_to_balco_trip or obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip:
        schedule_balco(date,special_schedule,delayDate=date)

    loadingUnloadingCheckJSG_delay()

    if sdate != None:
        plantEntry(date=date,specialSchedule3=True,sdate=sdate)
        obj1.sortedDict = updateSortedDict(specialSchedule3=True,sdate=sdate,date=date)
    else:
        plantEntry(date=date,specialSchedule=True)
        obj1.sortedDict = updateSortedDict(specialSchedule=True,date=date)

    obj1.timeSpend = 0
    obj1.resultTable.append(obj1.sortedDict)
    clearList()
    with open("a.txt","w") as f:
        f.write(json.dumps(obj1.sortedDict))

    dic = {}
    ls2 = [obj1.resultTable,
           obj1.ls_jsg_to_lnj_btap,
           obj1.ls_balco_to_lnj_btap,
           getGlobalList.hist_jsg_arrival,
           getGlobalList.hist_jsg_dep,
           getGlobalList.hist_lnj_arrival,
           getGlobalList.hist_lnj_dep,
           getGlobalList.hist_vizag_arrival,
           getGlobalList.hist_vizag_dep,
           getGlobalList.hist_kslk_arrival,
           getGlobalList.hist_kslk_dep,
           getGlobalList.hist_jsg_circuit,
           getGlobalList.hist_kslk_maintain,
           getGlobalList.hist_count_lnj_supply_jsg,
           getGlobalList.hist_count_lnj_supply_balco,
           getGlobalList.hist_count_kslk_supply_jsg,
           getGlobalList.hist_count_kslk_supply_balco,
           getGlobalList.hist_flag,
           getGlobalList.hist_txr_flag,
           getGlobalList.hist_store_txr,
           getGlobalList.hist_balco_arrival,
           getGlobalList.hist_balco_dep,
           getGlobalList.hist_count_balcoDep_not_lnjArr,
           getGlobalList.hist_count_jsgDep_not_lnjArr,
           getGlobalList.hist_plant1_demand,
           getGlobalList.hist_plant2_demand,
           getGlobalList.hist_plant_record,
           getGlobalList.btap_in_balco,
           getGlobalList.hist_hold_records,
           obj1.count_kslk_supply_jsg,
           obj1.count_kslk_supply_balco,
           obj1.count_lanj_supply_jsg,
           obj1.count_lanj_supply_balco,
           obj1.flag2,
           obj1.initial_cylon_capacity,
           obj1.vessel_arrival_date,
           obj1.cylon_capacity,
           obj1.lnj_jsg,
           obj1.lnj_balco,
           obj1.kslk_jsg,
           obj1.kslk_balco,
           obj1.demand_jsg,
           obj1.demand_balco,
           obj1.plant1_demand,
           obj1.plant2_demand,
           obj1.month,
           obj1.btap_capacity,
           obj1.btap_info,
           obj1.btap_txr_details,
           obj1.year
           ]

    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 13 for SQL Server};SERVER=wfsdeve.database.windows.net;DATABASE=vedanta ACT;UID=wfsdev;PWD=Tata@123")
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    connection = engine.connect()

    dic = {}
    for ind,val in enumerate(ls):
        if type(ls2[ind]) == int or dict:
            dic[val] = ls2[ind]
        else:
            dic[val] = ls2[ind][:]
    # pickle_out = open("outputForMonth2.pickle", "wb")
    # pickle.dump(dic, pickle_out)
    # pickle_out.close()
    y = json.dumps(dic,cls=DjangoJSONEncoder)
    connection.execute("DELETE FROM [vedanta ACT].dbo.dispatch_varibles_landing_table")
    connection.execute("INSERT INTO[vedanta ACT].dbo.dispatch_varibles_landing_table(json_data)VALUES('" + y + "');")
    connection.close()

    llk = []
    blist = ["VED-1","VED-2","VED-3","VED-4","VED-5","VED-6","VED-7","VED-8","VED-9","VED-10","VED-11","VED-12",
             "VED-13","VED-14","VED-15","VED-16","VED-17","VED-18","VED-19","VED-20","VED-21","VED-22","VED-23",
             "VED-24"]

    date = obj1.sys_date - timedelta(days=1)
    edate = monthrange(date.year,date.month)[1]

    resultTableUpdate = []
    for ind,val in enumerate(obj1.resultTable):
        if ind + 1 > edate:
            continue
        for x in val:
            temp = []
            date = datetime.strptime("{0}-{1}-{2}".format(ind + 1,obj1.month,obj1.year),"%d-%m-%Y")
            temp.append(date)
            temp.append(obj1.rev_btap_mapping[x[0]])
            temp.append(x[1][2])
            temp.append(x[1][0])
            temp.append(x[1][1])
            if x[0] in getGlobalList.btap_in_balco:
                temp.append("BALCO")
            else:
                temp.append("JSG")
            temp.append(int(x[0][4:]))
            resultTableUpdate.append(temp)
    result_table_df = pd.DataFrame(resultTableUpdate,
                                   columns=["start_date","btap_name","loaded_unloaded_flag","next_destination",
                                            "time_to_travel","plant_name","btap_id"])
    result_table_df.to_sql('dispatch_landing_result_table_temp',if_exists='replace',con=engine,index=False)

    pl_records = []
    for h in getGlobalList.hist_hold_records[-1]:
        if h[1] != h[2]:
            if h[0] in getGlobalList.btap_in_balco:
                pl_records.append([h[1],h[2],obj1.rev_btap_mapping[h[0]],h[3],h[4],"BALCO"])
            else:
                pl_records.append([h[1],h[2],obj1.rev_btap_mapping[h[0]],h[3],h[4],"JSG"])
    pl_records = sorted(pl_records,key=lambda x: x[1])
    hold_table_df = pd.DataFrame(pl_records,columns=["hold_entry_date","hold_exit_date","btap_name","current_location",
                                                     "start_date","plant_name"])
    hold_table_df.to_csv("HoldTable.csv")

    hold_table_df.to_sql('dispatch_landing_hold_table_temp',if_exists='replace',con=engine,index=False)

    pl_records = []
    for val in getGlobalList.hist_plant_record:
        for h in val:
            if h[0] in getGlobalList.btap_in_balco:
                pl_records.append([h[1],h[2],obj1.rev_btap_mapping[h[0]],h[3],"BALCO"])
            else:
                pl_records.append([h[1],h[2],obj1.rev_btap_mapping[h[0]],h[3],"JSG"])

    pl_records = filter_plant_records(pl_records[:])
    pl_records = sorted(pl_records,key=lambda x: x[1])
    plant_table_df = pd.DataFrame(pl_records,
                                  columns=['plant_entry_date','plant_exit_date','btap_name','plant_name',"plant_name"])

    plant_table_df.to_csv("plant.csv")

    plant_table_df.to_sql('dispatch_landing_plant_table_temp',if_exists='replace',con=engine,index=False)

    for i in range(len(blist)):
        bt = blist[i]
        llk.append([bt])
        for k in obj1.resultTable:
            for m in k:
                if m[0] == bt:
                    llk[-1].extend([str(m[1][0]) + " " + str(m[1][1]) + " " + str(m[1][2])])
    newData = pd.DataFrame(llk)
    newData.to_csv("withDelay.csv")

    dispatch_load = {}
    for val in pl_records:
        if val[3] in ["LNJ","KSLK-Plant"]:
            if val[2] not in dispatch_load:
                dispatch_load[val[2]] = 3000
            else:
                dispatch_load[val[2]] += 3000

    dispatch_load["LNJ-JSG-Supply"] = obj1.count_lanj_supply_jsg * 3000
    dispatch_load["KSLK-JSG-Supply"] = obj1.count_kslk_supply_jsg * 3000
    dispatch_load["LNJ-BALCO-Supply"] = obj1.count_lanj_supply_balco * 3000
    dispatch_load["KSLK-BALCO-Supply"] = obj1.count_kslk_supply_balco * 3000

    dispatch_supply = [[obj1.rev_btap_mapping[k],v] for k,v in dispatch_load.items()]
    dispatch_supply_df = pd.DataFrame(dispatch_supply,columns=["btap_name","btap_supply"])
    dispatch_supply_df.to_csv("dispatch_supply.csv")
    # print(dispatch_supply_df)
    # print(dispatch_load)
    #
    #
    # print("Lanji suply for JSG is ", obj1.count_lanj_supply_jsg)
    # print("KSLK supply for Jsg is ", obj1.count_kslk_supply_jsg)
    #
    # print("Lanji suply for balco is ", obj1.count_lanj_supply_balco)
    # print("KSLK supply for balco is ", obj1.count_kslk_supply_balco)

    sys.exit(0)
    return 0


def removeArguments(x):
    global obj1,getGlobalList
    del getGlobalList.hist_flag[-x:]
    del getGlobalList.hist_plant1_demand[-x:]
    del getGlobalList.hist_plant2_demand[-x:]
    del getGlobalList.hist_count_lnj_supply_balco[-x:]
    del getGlobalList.hist_count_lnj_supply_jsg[-x:]
    del getGlobalList.hist_count_kslk_supply_jsg[-x:]
    del getGlobalList.hist_count_kslk_supply_balco[-x:]
    del getGlobalList.hist_count_balcoDep_not_lnjArr[-x:]
    del getGlobalList.hist_count_jsgDep_not_lnjArr[-x:]
    del getGlobalList.hist_plant_record[-x:]
    del getGlobalList.hist_hold_records[-x:]


def dispatchSchedulerRunDelay(run_delay_choice,dateStart=None,dateEnd=None,delayls=None,delay_first_time=False,
                              run_delay_json=None):
    global obj1,getGlobalList
    obj1.sys_date = datetime.strptime("{0}-{1}-{2}".format(dateStart.day,dateStart.month,obj1.year),'%d-%m-%Y')
    obj1.resultTable.clear()
    retrievingArgument()
    obj1.demand_jsg = obj1.lnj_jsg + obj1.kslk_jsg
    obj1.demand_balco = obj1.lnj_balco + obj1.kslk_balco
    obj1.plant1_demand = math.ceil(obj1.demand_jsg / 3) // 3000
    obj1.plant2_demand = (obj1.demand_jsg - math.ceil(obj1.demand_jsg / 3)) // 3000
    obj1.compute_trips_and_btap(obj1.btap_capacity)
    days_in_month = monthrange(dateStart.year,dateStart.month)
    x = days_in_month[1] - dateStart.day + 1
    del obj1.resultTable[-x:]
    removeArguments(x - 1)
    obj1.sortedDict = obj1.resultTable[-1]
    obj1.holdlist = getGlobalList.hist_hold_records[-1][:]
    obj1.holdlist.append([delayls[0][0],dateStart,dateStart + timedelta(hours=delayls[0][1]),'Manual',obj1.sys_date])
    removeHistoryLnjData(dateStart)
    dat = dateStart.day
    h = dateStart.hour
    m = dateStart.minute
    mnt = dateStart.month
    date = datetime.strptime("{0}-{1}-{4} {2}:{3}:59".format(dat,mnt,h,m,obj1.year),'%d-%m-%Y %H:%M:%S')
    special_schedule = True
    # updating sorted dict
    clearList()
    updateBalcoSortedDict()
    updateLnjSortedDict()
    updateKslkSortedDict()
    sortThesorteddict()
    goKSLK()
    goLNJ()

    # Calling schedule
    if obj1.count_lanj_supply_jsg < obj1.lnj_to_jsg_trip or obj1.count_kslk_supply_jsg < obj1.kslk_to_jsg_trip:
        schedule(date,special_schedule,delayDate=date)
    if obj1.count_kslk_supply_balco < obj1.kslk_to_balco_trip or obj1.count_lanj_supply_balco < obj1.lnj_to_balco_trip:
        schedule_balco(date,special_schedule,delayDate=date)

    loadingUnloadingCheckJSG_delay()
    obj1.sortedDict = updateSortedDict(specialSchedule=True,date=date)
    obj1.timeSpend = 0
    plantEntry(date=date,specialSchedule=True)
    obj1.sys_date = date
    obj1.manual_delay_sortedDict = obj1.sortedDict[:]

    sdate = obj1.sys_date
    ls = []
    if delayls and dateEnd:
        ls = delayls[:]
    if sdate.day != dateEnd.day:
        while True:
            if dateEnd:
                ls = fun(sdate,dateEnd,ls[:],delayFirstTime=True,delayDate=dateStart,specialSchedule2=True)
            else:
                ls = fun(sdate,dateEnd,delay_first_time,ls[:])
            if len(ls) > 0:
                if ls[-1].day <= dateStart.day:  # check it
                    ls[-1] = dateStart
                sdate = ls[-1]
            else:
                break
    if dateEnd:
        if sdate.day == dateEnd.day:
            delaySortedDict(ls[:],delayFirstTime=True)
            dispatchSchedulerRunDelayAfter(dateEnd,run_delay_choice,run_delay_json,sdate=sdate)
        else:
            dispatchSchedulerRunDelayAfter(dateEnd,run_delay_choice,run_delay_json)


def delay(ch,delay_ui_json=None):
    global obj1,getGlobalList
    if ch.lower() == 'n' or ch.lower() == "b":
        return None,None
    else:

        u_date = delay_ui_json['u_date']
        format_d = datetime.strptime(u_date,'%d-%m-%Y %H:%M:%S')
        enddate1 = delay_ui_json['enddate1']
        enddate = datetime.strptime(enddate1,'%d-%m-%Y %H:%M:%S')
        u_btap_name = delay_ui_json['u_btap_name']
        u_load_s = delay_ui_json['u_load_s']
        u_dealy_hour = delay_ui_json['u_dealy_hour']
        delayls = [u_btap_name,format_d,u_load_s,u_dealy_hour]
        return delayls,enddate


def filter_plant_records(p1_records):
    global obj1,getGlobalList
    p1_records = [list(i) for i in set([tuple(i) for i in p1_records])]
    new_p1_records = copy.deepcopy(p1_records)
    f = 1
    while f == 1:
        pl_records_sorted = sorted(new_p1_records,key=operator.itemgetter(1,2))
        new_p1_records.clear()
        f = 0
        if len(pl_records_sorted) > 0:
            for ind,val in enumerate(pl_records_sorted):
                if ind == 0:
                    old_ved = val[0]
                    old_dest = val[3]
                    start_plant = val[1]
                    end_plant = val[2]
                    continue
                if val[0] == old_ved and val[3] == old_dest and val[1] > start_plant and val[1] <= end_plant:
                    new_p1_records.append([val[0],start_plant,val[2],val[3]])
                    start_plant = start_plant
                    f = 1
                else:
                    new_p1_records.append(val)
                    start_plant = val[1]
                old_ved = val[0]
                old_dest = val[3]
                end_plant = val[2]
    new_p1_records = [list(i) for i in set([tuple(i) for i in new_p1_records])]
    return new_p1_records[:]


def output():
    global obj1,getGlobalList
    print("obj1.flag",obj1.flag)
    print("len(obj1.flag)",len(obj1.flag))
    ls2 = [obj1.resultTable,
           obj1.ls_jsg_to_lnj_btap,
           obj1.ls_balco_to_lnj_btap,
           getGlobalList.hist_jsg_arrival,
           getGlobalList.hist_jsg_dep,
           getGlobalList.hist_lnj_arrival,
           getGlobalList.hist_lnj_dep,
           getGlobalList.hist_vizag_arrival,
           getGlobalList.hist_vizag_dep,
           getGlobalList.hist_kslk_arrival,
           getGlobalList.hist_kslk_dep,
           getGlobalList.hist_jsg_circuit,
           getGlobalList.hist_kslk_maintain,
           getGlobalList.hist_count_lnj_supply_jsg,
           getGlobalList.hist_count_lnj_supply_balco,
           getGlobalList.hist_count_kslk_supply_jsg,
           getGlobalList.hist_count_kslk_supply_balco,
           getGlobalList.hist_flag,
           getGlobalList.hist_txr_flag,
           getGlobalList.hist_store_txr,
           getGlobalList.hist_balco_arrival,
           getGlobalList.hist_balco_dep,
           getGlobalList.hist_count_balcoDep_not_lnjArr,
           getGlobalList.hist_count_jsgDep_not_lnjArr,
           getGlobalList.hist_plant1_demand,
           getGlobalList.hist_plant2_demand,
           getGlobalList.hist_plant_record,
           getGlobalList.btap_in_balco,
           getGlobalList.hist_hold_records,
           obj1.count_kslk_supply_jsg,
           obj1.count_kslk_supply_balco,
           obj1.count_lanj_supply_jsg,
           obj1.count_lanj_supply_balco,
           obj1.flag2,
           obj1.initial_cylon_capacity,
           obj1.vessel_arrival_date,
           obj1.cylon_capacity,
           obj1.lnj_jsg,
           obj1.lnj_balco,
           obj1.kslk_jsg,
           obj1.kslk_balco,
           obj1.demand_jsg,
           obj1.demand_balco,
           obj1.plant1_demand,
           obj1.plant2_demand,
           obj1.month,
           obj1.btap_capacity,
           obj1.btap_info,
           obj1.btap_txr_details,
           obj1.year
           ]

    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 13 for SQL Server};SERVER=wfsdeve.database.windows.net;DATABASE=vedanta ACT;UID=wfsdev;PWD=Tata@123")
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    connection = engine.connect()

    dic = {}
    for ind,val in enumerate(ls):
        if type(ls2[ind]) == int or dict:
            dic[val] = ls2[ind]
        else:
            dic[val] = ls2[ind][:]
    y = json.dumps(dic,cls=DjangoJSONEncoder)
    # y = sjson.dumps(dic)
    connection.execute("DELETE FROM[vedanta ACT].dbo.dispatch_varibles_main_table;")
    connection.execute("INSERT INTO[vedanta ACT].dbo.dispatch_varibles_main_table(json_data)VALUES('" + y + "');")
    connection.close()
    # pickle_out = open("outputForMonth.pickle", "wb")
    # pickle.dump(dic, pickle_out)
    # pickle_out.close()

    date = obj1.sys_date - timedelta(days=1)
    edate = monthrange(date.year,date.month)[1]

    resultTableUpdate = []
    for ind,val in enumerate(obj1.resultTable):
        if ind + 1 > edate:
            continue
        for x in val:
            temp = []
            date = datetime.strptime("{0}-{1}-{2}".format(ind + 1,obj1.month,obj1.year),"%d-%m-%Y")
            temp.append(date)
            temp.append(obj1.rev_btap_mapping[x[0]])
            temp.append(x[1][2])
            temp.append(x[1][0])
            temp.append(x[1][1])
            if x[0] in getGlobalList.btap_in_balco:
                temp.append("BALCO")
            else:
                temp.append("JSG")
            temp.append(int(x[0][4:]))

            resultTableUpdate.append(temp)
    result_table_df = pd.DataFrame(resultTableUpdate,
                                   columns=["start_date","btap_name","loaded_unloaded_flag","next_destination",
                                            "time_to_travel","plant_name","btap_id"])
    result_table_df.to_csv("resultTable.csv")
    # result_table_df.to_sql('dispatch_main_result_table_temp', if_exists='replace', con=engine, index=False)

    pl_records = []
    for h in getGlobalList.hist_hold_records[-1]:
        if h[1] != h[2]:
            if h[0] in getGlobalList.btap_in_balco:
                pl_records.append([h[1],h[2],obj1.rev_btap_mapping[h[0]],h[3],h[4],"BALCO"])
            else:
                pl_records.append([h[1],h[2],obj1.rev_btap_mapping[h[0]],h[3],h[4],"JSG"])
    pl_records = sorted(pl_records,key=lambda x: x[1])
    hold_table_df = pd.DataFrame(pl_records,columns=["hold_entry_date","hold_exit_date","btap_name","current_location",
                                                     "start_date","plant_name"])
    hold_table_df.to_csv("HoldTable.csv")

    # hold_table_df.to_sql('dispatch_main_hold_table_temp', if_exists='replace', con=engine, index=False)

    pl_records = []
    for val in getGlobalList.hist_plant_record:
        for h in val:
            if h[0] in getGlobalList.btap_in_balco:
                pl_records.append([h[1],h[2],obj1.rev_btap_mapping[h[0]],h[3],"BALCO"])
            else:
                pl_records.append([h[1],h[2],obj1.rev_btap_mapping[h[0]],h[3],"JSG"])

    pl_records = filter_plant_records(pl_records[:])
    pl_records = sorted(pl_records,key=lambda x: x[1])
    plant_table_df = pd.DataFrame(pl_records,
                                  columns=['plant_entry_date','plant_exit_date','btap_name','plant_name',"plant_name"])
    plant_table_df.to_csv("plant.csv")

    # plant_table_df.to_sql('dispatch_main_plant_table_temp', if_exists='replace', con=engine, index=False)

    # df.to_sql('dispatch_schedular_plant_entry', con=engine, if_exists='replace',index = False)

    llk = []
    blist = ["VED-1","VED-2","VED-3","VED-4","VED-5","VED-6","VED-7","VED-8","VED-9","VED-10","VED-11","VED-12",
             "VED-13","VED-14","VED-15","VED-16","VED-17","VED-18","VED-19","VED-20","VED-21","VED-22","VED-23",
             "VED-24"]

    p1_records = [list(i) for i in set([tuple(i) for i in pl_records])]
    pl_records = []
    for val in getGlobalList.hist_plant_record:
        for h in val:
            if h[0] in getGlobalList.btap_in_balco:
                pl_records.append([h[1],h[2],h[0],h[3],"BALCO"])
            else:
                pl_records.append([h[1],h[2],h[0],h[3],"JSG"])

    pl_records = filter_plant_records(pl_records[:])
    pl_records = sorted(pl_records,key=lambda x: x[1])
    dispatch_load = {}
    for val in pl_records:
        if val[3] in ["LNJ","KSLK-Plant"]:
            if val[2] not in dispatch_load:
                dispatch_load[val[2]] = 3000
            else:
                dispatch_load[val[2]] += 3000

    dispatch_btap_supply = []
    # dispatch_btap_supply = [[k, v] for k, v in dispatch_load.items()]
    for k,v in dispatch_load.items():
        print("K",k)
        if k in getGlobalList.btap_in_balco:
            dispatch_btap_supply.append([obj1.rev_btap_mapping[k],v,"BALCO"])
        else:
            dispatch_btap_supply.append([obj1.rev_btap_mapping[k],v,"JSG"])

    dispatch_btap_supply_df = pd.DataFrame(dispatch_btap_supply,columns=["btap_name","btap_supply","plant_name"])
    dispatch_btap_supply_df.to_csv("dispatch_btap_supply.csv")
    # dispatch_btap_supply_df.to_sql('dispatch_btap_supply_temp', if_exists='replace', con=engine, index=False)

    dispatch_plant_load = []
    dispatch_plant_load.append(["LNJ-JSG-Supply",obj1.count_lanj_supply_jsg * 3000])
    dispatch_plant_load.append(["KSLK-JSG-Supply",obj1.count_kslk_supply_jsg * 3000])
    dispatch_plant_load.append(["LNJ-BALCO-Supply",obj1.count_lanj_supply_balco * 3000])
    dispatch_plant_load.append(["KSLK-BALCO-Supply",obj1.count_kslk_supply_balco * 3000])

    dispatch_plant_supply_df = pd.DataFrame(dispatch_plant_load,columns=["btap_name","btap_supply"])
    dispatch_plant_supply_df.to_csv("dispatch_plant_supply_temp.csv")
    dispatch_plant_supply_df.to_sql('dispatch_plant_supply_temp',if_exists='replace',con=engine,index=False)

    for i in range(len(blist)):
        bt = blist[i]
        llk.append([bt])
        for k in obj1.resultTable:
            for m in k:
                if m[0] == bt:
                    llk[-1].extend([str(m[1][0]) + " " + str(m[1][1]) + " " + str(m[1][2])])
    newData = pd.DataFrame(llk)
    newData.to_csv("withoutDelay.csv")

    # print("Lanji suply for JSG is ", obj1.count_lanj_supply_jsg)
    # print("KSLK supply for Jsg is ", obj1.count_kslk_supply_jsg)
    # print("Lanji suply for balco is ", obj1.count_lanj_supply_balco)
    # print("KSLK supply for balco is ", obj1.count_kslk_supply_balco)


# Hello Test User
def run(s_date,delay_choice,delay_ui_json,run_delay_choice,run_delay_json):
    global obj1,getGlobalList
    delayFirstTime = True
    vesselToCylo()
    delayls,dateEnd = delay(delay_choice,delay_ui_json)
    obj1.sys_date = datetime.strptime(s_date,"%d-%m-%Y %H:%M:%S")

    if obj1:
        pass

    if not delayls:
        dispatchSchedulerRun(delay_choice)
    else:
        delaylist = []
        if delayls[2] == 'L':
            newdate = delayls[1] - timedelta(hours=delayls[3])
            if newdate.month != delayls[1].month:
                month = delayls[1].month
                newdate = datetime.strptime("01-{0}-{1}".format(month,obj1.year),'%d-%m-%Y ')
            delaylist.append([delayls[0],delayls[3]])
        else:
            newdate = delayls[1]
            delaylist.append([delayls[0],- delayls[3]])
        dispatchSchedulerRunDelay(run_delay_choice,newdate,dateEnd,delaylist,delayFirstTime,run_delay_json)
    output()


def dispatch_controller(s_date,delay_choice,lnj_jsg=None,kslk_jsg=None,lnj_balco=None,kslk_balco=None,
                        run_delay_choice=None,
                        run_delay_json=None,delay_ui_json=None,no_of_btap=None):
    global obj1,getGlobalList

    config_json = read_input_json('config_dispatch.json')
    vessel_arrival_date = config_json['vessel_arrival_date']
    capacity = config_json['btap_capacity']

    obj1 = DispatchScheduler(lnj_jsg=lnj_jsg,kslk_jsg=kslk_jsg,lnj_balco=lnj_balco,
                             kslk_balco=kslk_balco,btap_capacity=capacity,no_of_btap=no_of_btap)

    date_for_month = datetime.strptime(s_date,"%d-%m-%Y %H:%M:%S")
    obj1.month = date_for_month.month
    obj1.year = date_for_month.year
    obj1.tat_dict = config_json['tat_dict']
    # obj1.btap_info = config_json['btap_info']
    obj1.initial_cylon_capacity = config_json['initial_cylon_capacity']
    getGlobalList = ArrivalDeparture()

    if delay_choice.lower() == "n":
        obj1.demand_jsg = obj1.lnj_jsg + obj1.kslk_jsg
        obj1.demand_balco = obj1.lnj_balco + obj1.kslk_balco
        obj1.plant1_demand = math.ceil(obj1.demand_jsg / 3) // 3000
        obj1.plant2_demand = (obj1.demand_jsg - math.ceil(obj1.demand_jsg / 3)) // 3000
        obj1.btap_capacity = capacity
        obj1.compute_trips_and_btap(obj1.btap_capacity)
        no_of_btap = input_json["no_of_btap"]
        i = 0
        obj1.btap_info = {}
        obj1.btap_txr_details = {}
        getGlobalList.btap_in_balco.clear()
        while i < no_of_btap:

            btap_name = input_json["btap_info"][i]["btap_name"]
            destination = input_json["btap_info"][i]["destination"]
            tat = input_json["btap_info"][i]["tat"]
            load_status = input_json["btap_info"][i]["load_status"]
            TXR = input_json["btap_info"][i]["txr"]
            btap_category = input_json["btap_info"][i]["btap_category"]
            i += 1
            obj1.btap_mapping[btap_name] = "VED-" + str(i)
            obj1.rev_btap_mapping["VED-" + str(i)] = btap_name
            btap_name = obj1.btap_mapping[btap_name]
            if btap_category == "BALCO":
                getGlobalList.btap_in_balco.append(btap_name)

            obj1.btap_info[btap_name] = [load_status,destination,tat,btap_category]
            # "2019-08-17T06:30:00.000Z"
            TXR = TXR.split('T')[0]
            obj1.btap_txr_details[btap_name] = datetime.strptime(TXR,"%Y-%m-%d")

        print("obj1.rev_btap_mapping", obj1.rev_btap_mapping)
    # for key,val in btap_txr_details.items():
    #     obj1.btap_txr_details[key] = datetime.strptime(val,'%d-%m-%Y')
    for val in vessel_arrival_date:
        obj1.vessel_arrival_date.append(datetime.strptime(val, '%d-%m-%Y'))

    if delay_choice.lower() == "b":
        if run_delay_choice.lower() == "y":
            retrievingArgument(True)
            obj1.demand_jsg = obj1.lnj_jsg + obj1.kslk_jsg
            obj1.demand_balco = obj1.lnj_balco + obj1.kslk_balco
            obj1.plant1_demand = math.ceil(obj1.demand_jsg / 3) // 3000
            obj1.plant2_demand = (obj1.demand_jsg - math.ceil(obj1.demand_jsg / 3)) // 3000
            obj1.compute_trips_and_btap(obj1.btap_capacity)
            obj1.sortedDict = obj1.resultTable[-1]
            obj1.holdlist = getGlobalList.hist_hold_records[-1]
            obj1.plantdates = getGlobalList.hist_plant_record[-1]
            del obj1.resultTable[-1]
            del getGlobalList.hist_plant_record[-1]
            del getGlobalList.hist_hold_records[-1]

        else:
            add_ui_database_data(run_delay_json)

    eDate = monthrange(date_for_month.year, date_for_month.month)[1]
    obj1.flag = [0] * obj1.no_of_btap
    obj1.cylon_capacity = [obj1.initial_cylon_capacity] * eDate
    run(s_date, delay_choice, delay_ui_json, run_delay_choice, run_delay_json)
    saveSortedDf = pd.DataFrame(obj1.sortedDict)
    saveSortedDf.to_csv("ContinuousMonthSortedDict.csv")


def add_ui_database_data(run_delay_json):
    global obj1, getGlobalList
    retrievingArgument(True)
    obj1.demand_jsg = obj1.lnj_jsg + obj1.kslk_jsg
    obj1.demand_balco = obj1.lnj_balco + obj1.kslk_balco
    obj1.plant1_demand = math.ceil(obj1.demand_jsg / 3) // 3000
    obj1.plant2_demand = (obj1.demand_jsg - math.ceil(obj1.demand_jsg / 3)) // 3000

    obj1.compute_trips_and_btap(obj1.btap_capacity)
    obj1.sortedDict = obj1.resultTable[-1]
    del obj1.resultTable[-1]
    btap_c = int(run_delay_json['btap_to_correct'])
    sorted_len = len(obj1.sortedDict)
    while btap_c != 0:
        btap_n = run_delay_json['btap_info'][btap_c - 1]["btap_name"]
        btap_d = run_delay_json['btap_info'][btap_c - 1]["destination"]
        btap_tat = run_delay_json['btap_info'][btap_c - 1]["tat"]
        btap_c -= 1
        for idx in range(sorted_len):
            if obj1.sortedDict[idx][0].upper() == btap_n.upper():
                obj1.sortedDict[idx][1][0] = btap_d
                obj1.sortedDict[idx][1][1] = btap_tat


if __name__ == "__main__":
    input_json = read_input_json()
    delay_choice = input_json['delay_choice']
    run_delay_choice = input_json['run_delay_choice']
    delay_ui_json = input_json['delay_ui_json']
    run_delay_json = input_json['run_delay_json']
    s_date = input_json['s_date']

    if delay_choice.lower() == "n":
        lnj_jsg = input_json['lnj_jsg']
        kslk_jsg = input_json['kslk_jsg']
        lnj_balco = input_json['lnj_balco']
        kslk_balco = input_json['kslk_balco']
        no_of_btap = input_json['no_of_btap']

        dispatch_controller(s_date,lnj_jsg=lnj_jsg,kslk_jsg=kslk_jsg,lnj_balco=lnj_balco,kslk_balco=kslk_balco,
                            delay_choice=delay_choice,run_delay_choice=run_delay_choice,
                            run_delay_json=run_delay_json,delay_ui_json=delay_ui_json,no_of_btap=no_of_btap)

    else:
        dispatch_controller(s_date,delay_choice=delay_choice,run_delay_choice=run_delay_choice,
                            run_delay_json=run_delay_json,delay_ui_json=delay_ui_json)
