import time
import random
from random import randint
from time import sleep
import pandas as pd
import numpy as np
from mpi4py import MPI
from scipy.stats import ks_2samp
from sklearn.preprocessing import StandardScaler

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def k_random_sample(data, k):
    # Takes a dataframe and an number of observations
    # returns new dataframe containing k from n pseudorandom
    # observations with out replacement

    n = len(data)
    indices = random.sample(range(0, n), k)
    return data.iloc[indices]


def ks_test(master_df, sample, weather_variables):
    
    ks_pvals = []

    for variable in weather_variables:
        master_data = np.array(master_df[variable])
        sample_data = np.array(sample[variable])

        try:
            ks_result = ks_2samp(master_data, sample_data)
            
        except ValueError:
            ks_pvals.append(0.0)
            
        else:
            ks_pvals.append(ks_result[1])
    
    return ks_pvals


def replace_sample_subset(master_df, sample, fraction):  
    # split sample into positive and negative
    sample_positive = sample[sample['ignition'] == 1]
    sample_negative = sample[sample['ignition'] == 0]

    # choose n random indexes to replace in each
    n_positive = len(sample_positive)
    k_positive = int(len(sample_positive)*fraction)
    indices_positive = random.sample(range(0, n_positive), k_positive)

    n_negative = len(sample_negative)
    k_negative = int(len(sample_negative)*fraction)
    indices_negative = random.sample(range(0, n_negative), k_negative)

    # grab data to replace and remove it from the sample
    data_to_replace_positive = sample_positive.iloc[indices_positive]
    ids_to_replace_positive = data_to_replace_positive.ID
    sample_positive = sample_positive[~sample_positive.ID.isin(ids_to_replace_positive)]

    data_to_replace_negative = sample_negative.iloc[indices_negative]
    ids_to_replace_negative = data_to_replace_negative.ID
    sample_negative = sample_negative[~sample_negative.ID.isin(ids_to_replace_negative)]

    # split parent data into positive and negative
    parent_positive = master_df[master_df['ignition'] == 1]
    parent_negative = master_df[master_df['ignition'] == 0]

    # grab replacement points
    eligible_parent_positive = sample_positive[~sample_positive.ID.isin(ids_to_replace_positive)]
    new_sample_positive = k_random_sample(eligible_parent_positive, k_positive)

    eligible_parent_negative = sample_negative[~sample_negative.ID.isin(ids_to_replace_negative)]
    new_sample_negative = k_random_sample(eligible_parent_negative, k_negative)

    # Add new points to sample and old points back to parent dataset
    sample_positive = sample_positive.append(new_sample_positive)
    sample_negative = sample_negative.append(new_sample_negative)
    sample = sample_positive.append(sample_negative)

    return sample


def make_starting_sample(master_df, sample_size):
    # determine fraction positive observations in master data
    fraction_positive = len(master_df[master_df['ignition'] == 1])/len(master_df)

    # split positive and negative datsets up
    ignitions = master_df[master_df['ignition'] == 1]
    no_ignitions = master_df[master_df['ignition'] == 0]

    # Calculate ignition & no ignition sample sizes
    ignition_fraction = len(ignitions) / len(master_df)
    ignition_sample_size = int((sample_size * ignition_fraction))
    no_ignition_sample_size = int((sample_size * (1 - ignition_fraction)))

    # sample data
    no_ignitions_sample = k_random_sample(no_ignitions, no_ignition_sample_size)
    ignitions_sample = k_random_sample(ignitions, ignition_sample_size)

    # combine
    sample = no_ignitions_sample.append(ignitions_sample)
    
    return sample


def enum(*sequential, **named):
    """Handy way to fake an enumerated type in Python
    http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

# Define MPI message tags
tags = enum('READY', 'DONE', 'EXIT', 'START')

# Initializations and preliminaries
comm = MPI.COMM_WORLD   # get MPI communicator object
size = comm.size        # total number of processes
rank = comm.rank        # rank of this process
status = MPI.Status()   # get MPI status object

data_file = '../data/training_data/1992-2015_training_data_raw.csv'

dtypes={
    'air.sfc': float,
    'air.2m': float,
    'apcp': float,
    'crain': float,
    'rhum.2m': float,
    'dpt.2m': float,
    'pres.sfc': float,
    'uwnd.10m': float,
    'vwnd.10m': float,
    'veg': float,
    'prate': float,
    'vis': float,
    'lat': float,
    'lon': float,
    'weather_bin_month': int,
    'weather_bin_year': int,
    'ignition': float
}

weather_variables = [
    'air.sfc',
    'air.2m', 
    'apcp',
    'crain',
    'rhum.2m',
    'dpt.2m',
    'pres.sfc',
    'uwnd.10m', 
    'vwnd.10m',
    'veg',
    'prate',
    'vis',
    'lat',
    'lon',
    'weather_bin_month',
    'weather_bin_year'
]

file_read_chunksize = 1000
sample_size = 10000
fraction = 0.01
sample_round = 0
target_pval = 0.7
target_pvals = [target_pval] * len(weather_variables)
old_total_distance = len(weather_variables)

excluded_observations = []
sample_ids = []

output_file_base_name = "../data/stratified_training_data/1992-2015_training_data_raw_n" \
    +str(sample_size) \
    +"_ks" \
    +str(target_pval) \
    +"."

for worker_num in range(0,size):
    if rank == worker_num:
        name = MPI.Get_processor_name()
        print("{}-{} loading dataframe".format(name, rank))
        
        master_df = pd.DataFrame()
        for chunk in pd.read_csv(data_file, dtype=dtypes, chunksize=file_read_chunksize):
            master_df = pd.concat([master_df, chunk], ignore_index=True)        
            
        master_df.insert(0, 'ID', range(0, len(master_df)))
        print("{}-{} done loading".format(name, rank))

start_time = time.time()

if rank == 0:
    # Grab and score starting sample
    sample = make_starting_sample(master_df, sample_size)
    sample_ids = sample['ID']
    ks_pvals = ks_test(master_df, sample, weather_variables)
    distances = np.subtract(target_pvals, ks_pvals)
    total_distance = sum(np.maximum(distances, 0))
    old_total_distance = total_distance
    
    print()
    print("Number of processes: {}".format(size))
    print("Done loading and prepping data")
    print("Master df size: {}".format(len(master_df)))
    print("Sample size: {}".format(len(sample)))
    print("Starting total distance: {}".format(np.round(old_total_distance,2)))
    print()

    while True:
        # Master process executes code below
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()

        if tag == tags.READY:
            # assemble workunit
            workunit = [
                old_total_distance,
                sample_round,
                sample_ids,
                excluded_observations,
            ]

            # send workunit to nodes
            comm.send(workunit, dest=source, tag=tags.START)

        elif tag == tags.DONE:
            # unpack results
            result = data
            worker_ks_pvals = result[0]
            worker_total_distance = result[1]
            worker_sample_round = result[2]
            worker_sample_ids = result[3]
            worker_name = result[4]
            
            dT = np.round(((time.time() - start_time) / 3600),2)

            # if distance from worker wins, update
            if worker_total_distance < old_total_distance and worker_sample_round == sample_round:
                print("Winning distance from {}-{}, round {}: {}, dT: {}".format(
                    worker_name,
                    source,
                    worker_sample_round, 
                    np.round(worker_total_distance, 2),
                    dT)
                )
                
                print()
                
                sample_ids = worker_sample_ids
                old_total_distance = worker_total_distance

                # if all pvalues from this sample pass threshold, write to file, exclude observations
                # from future samples and reset
                if all(p_vals >= 0.9 for p_vals in worker_ks_pvals) and worker_sample_round == sample_round:
                    # write winner to disk
                    print("Winner winner chicken dinner, round: {}".format(sample_round))
                    print()
                    output_file = str(output_file_base_name)+str(sample_round)+'.csv'
                    sample = master_df[master_df.ID.isin(sample_ids)]
                    sample.to_csv(output_file, index=False)

                    # Exclude members of the winning sample from future samples
                    excluded_observations = sample['ID']
                    master_df = master_df[~master_df.ID.isin(excluded_observations)]
                    
                    # Reset and on to the next round
                    sample = make_starting_sample(master_df, sample_size)
                    ks_pvals = ks_test(master_df, sample, weather_variables)
                    distances = np.subtract(target_pvals, ks_pvals)
                    total_distance = sum(np.maximum(distances, 0))

else:
    # Worker processes execute code below
    name = MPI.Get_processor_name()
    
    # Desyncronize node starts by 1-60 seconds
    time.sleep((random.randint(10, 600) / 10))
    
    while True:
        comm.send(None, dest=0, tag=tags.READY)
        workunit = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        
        if tag == tags.START:
            # Unpack workunit
            old_total_distance = workunit[0]
            sample_round = workunit[1]
            sample_ids = workunit[2]
            excluded_observations = workunit[3]
            
#             print("{} starting with   {}".format(
#                 rank, 
#                 np.round(old_total_distance,2)
#             ))
            
#             print()
        
            master_df = master_df[~master_df.ID.isin(excluded_observations)]
            sample = master_df[master_df.ID.isin(sample_ids)]
            
            for i in range(1):
                sample = replace_sample_subset(master_df, sample, fraction)
                ks_pvals = ks_test(master_df, sample, weather_variables)
                distances = np.subtract(target_pvals, ks_pvals)
                total_distance = sum(np.maximum(distances, 0))
                
#                 print("{} returning dist. {}".format(
#                     rank, 
#                     np.round(total_distance,2)
#                 ))
                
                if total_distance < old_total_distance:
                    # if we win, break the for loop to call home imediatly                    
                    break
                    
            # after n trials assemble result
            result = []
            result.append(ks_pvals)
            result.append(total_distance)
            result.append(sample_round)
            result.append(sample_ids)
            result.append(name)

            # send result to headnode
            comm.send(result, dest=0, tag=tags.DONE)
