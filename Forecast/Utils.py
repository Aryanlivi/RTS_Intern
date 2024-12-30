from decimal import Decimal
import numpy as np
import math
def calculate_discharge_galchi(water_level):
    water_level=Decimal(str(water_level))
    a=Decimal(174.033420951409)
    b=Decimal(1.23635884471447)
    c=Decimal(365.6)
    difference = water_level - c
    power=difference**b
    discharge=a*power
    return float(discharge)


def calculate_discharge_budhi(water_level):
    # a=417.1536
    # b=0.36303508
    # c=333.56
    water_level=Decimal(str(water_level))
    a=Decimal(417.1536)
    b=Decimal(0.36303508)
    c=Decimal(333.56)
    difference = water_level - c
    power= difference**b
    discharge= a*power
    return float(discharge)


def calculate_discharge_suirenitar(water_level):
    water_level=Decimal(str(water_level))
    a=Decimal(657.5618)
    # b=Decimal(0.46969843)
    b=Decimal(0.65)
    c=Decimal(280.12)
    difference = water_level - c
    power= difference**b
    discharge= a*power
    return float(discharge)

def calculate_mean_velocity_galchi(discharge):
    return 0.002*discharge+1.142

def calculate_mean_velocity_buddhi(discharge):
    return (0.002851381701*discharge)+0.646

def calcualte_time(distance,velocity):
    if np.isnan(velocity) or velocity == 0:
        return np.nan  
    
    time_delay = distance / (velocity*3600)
    # Separate time into hours and minutes
    hours = int(time_delay)
    minutes = (time_delay - hours) * 60
    
    # Round minutes to the nearest 10-minute interval
    minutes = (minutes / 10) * 10
    
    return hours, minutes


