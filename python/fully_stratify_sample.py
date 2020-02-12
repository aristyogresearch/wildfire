import random
from time import sleep
import pandas as pd
import numpy as np
from mpi4py import MPI
from scipy.stats import ks_2samp

data_file = '../data/training_data/1992-1997_training_data_daily_mean.csv'

sample_size = 10000
old_ks = 0
fraction = 0.001
ks_threshold = 21
sample_round = 0

weather_variables = [
    'weather_bin_month','weather_bin_year','air.sfc', 'air.2m',
    'apcp', 'crain', 'rhum.2m', 'dpt.2m', 'pres.sfc', 'uwnd.10m',
    'vwnd.10m', 'veg', 'dlwrf', 'dswrf', 'lcdc','hcdc', 'mcdc',
    'hpbl', 'prate', 'vis', 'ulwrf.sfc'
]

def k_random_sample(data, k):
    # Takes a data frame and an number of observations
    # returns dataframe containing k from n pseudorandom
    # observations with out replacement

    n = len(data)
    indices = random.sample(range(0, n), k)
    return data.iloc[indices]

def cal_KS_total_statistic(data, sample_data, weather_variables):

    total_ks = 0

    for variable in weather_variables:
        parent_data = np.array(data[variable])
        sample_data = np.array(sample[variable])

        ks_result = ks_2samp(parent_data, sample_data)
        total_ks = total_ks + ks_result[1]

    return total_ks

def replace_sample_subset(sample, fraction, data):
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
        parent_positive = data[data['ignition'] == 1]
        parent_negative = data[data['ignition'] == 0]

        # grab replacement points and remove them from the parent dataset
        # note: observations from last round are not in the parent dataset
        new_sample_positive = k_random_sample(parent_positive, k_positive)
        sample_ids_positive = sample_positive.ID
        parent_positive = parent_positive[~parent_positive.ID.isin(sample_ids_positive)]

        new_sample_negative = k_random_sample(parent_negative, k_negative)
        sample_ids_negative = sample_negative.ID
        parent_negative = parent_negative[~parent_negative.ID.isin(sample_ids_negative)]

        # Add new points to sample and old points back to parent dataset
        sample_positive = sample_positive.append(new_sample_positive)
        sample_negative = sample_negative.append(new_sample_negative)
        sample = sample_positive.append(sample_negative)

        parent_positive = parent_positive.append(data_to_replace_positive)
        parent_negative = parent_negative.append(data_to_replace_negative)
        data = parent_positive.append(parent_negative)

        return data, sample

def make_starting_sample(data, sample_size):
    # make a master copy of the data
    master_data = data.copy()

    # determine fraction positive observations in master data
    fraction_positive = len(master_data[master_data['ignition'] == 1])/len(master_data)

    # split positive and negative datsets up
    ignitions = data[data['ignition'] == 1]
    no_ignitions = data[data['ignition'] == 0]

    # Calculate ignition & no ignition sample sizes
    ignition_fraction = len(ignitions) / len(data)
    ignition_sample_size = int((sample_size * ignition_fraction))
    no_ignition_sample_size = int((sample_size * (1 - ignition_fraction)))

    # sample data
    no_ignitions_sample = k_random_sample(no_ignitions, no_ignition_sample_size)
    ignitions_sample = k_random_sample(ignitions, ignition_sample_size)

    # combine
    sample = no_ignitions_sample.append(ignitions_sample)

    # in the first round, our sample is the winning sample
    winning_sample = sample.copy()

    # get IDs of observations in sample
    sample_ids = sample.ID

    # remove sample observations from parent dataset
    data = data[~data.ID.isin(sample_ids)]

    return sample, winning_sample, data, master_data

def check_ks(total_ks, old_ks, sample, winning_sample, fraction, data):
    if total_ks > old_ks:
        # if we win, replace winning sample
        #print("New winner total KS statistic: {}".format(np.round(total_ks,3)))
        winning_sample = sample
        old_ks = total_ks

        # Then resample and move on
        data, sample = replace_sample_subset(sample, fraction, data)

    else:
        # If we did not win, go back to our last winner, resample and move on
        sample = winning_sample
        #print("Total KS statistic: {}".format(np.round(total_ks,3)))
        data, sample = replace_sample_subset(sample, fraction, data)

    return(total_ks, old_ks, sample, winning_sample, data)

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

if rank == 0:
    print("Number of processes: {}".format(size))
    # # Load & prep data
    print("Ready to load data")
    parent_data = pd.read_csv(data_file, low_memory=False)
    parent_data = parent_data.sample(frac=1).reset_index(drop=True)
    parent_data.insert(0, 'ID', range(0, len(parent_data)))
    print("Done loading and prepping data")

    # # take starting sample
    sample, winning_sample, parent_data, master_data = make_starting_sample(parent_data, sample_size)
    total_ks = cal_KS_total_statistic(master_data, sample, weather_variables)
    total_ks, old_ks, sample, winning_sample, parent_data = check_ks(total_ks, old_ks, sample, winning_sample, fraction, parent_data)

    print("Starting ks: {}".format(old_ks))

    while True:
        # Master process executes code below
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()

        if tag == tags.READY:

            # assemble workunit
            workunit = [
                master_data, 
                parent_data, 
                sample,
                weather_variables, 
                total_ks, 
                old_ks,
                fraction,
                sample_round
            ]

            # send workunit to nodes
            comm.send(workunit, dest=source, tag=tags.START)

        elif tag == tags.DONE:
            # unpack results
            result = data
            worker_parent_data = result[0]
            worker_winning_sample = result[1]
            worker_ks = result[2]
            worker_sample_round = result[3]

            # if KS score from worker wins, update
            if worker_ks > old_ks:
                print("Winning KS from worker {} round {}: {}".format(source, worker_sample_round, worker_ks))
                parent_data = worker_parent_data
                sample = worker_winning_sample
                winning_sample = worker_winning_sample
                old_ks = worker_ks

                # if this sample passes threshold, write to file, exclude observations
                # from future samples and reset
                if worker_ks >= ks_threshold and worker_sample_round == sample_round:

                    # write winner to disk
                    print("Winner winner chicken dinner, round: {}".format(sample_round))
                    output_file = 'sample.'+str(sample_round)+'.csv'
                    sample.to_csv(output_file, index=False)
                    sample_round += 1

                    # exclude observations in winning sample
                    excluded_indicies = sample.ID.values
                    parent_data = parent_data[~parent_data.ID.isin(excluded_indicies)]
                    
                    # reset for next workunit
                    old_ks = 0
                    sample, winning_sample, parent_data, master_data = make_starting_sample(
                        parent_data,
                        sample_size
                    )
                    total_ks = cal_KS_total_statistic(
                        master_data,
                        sample,weather_variables
                    )
                    total_ks, old_ks, sample, winning_sample, parent_data = check_ks(
                        total_ks, 
                        old_ks, 
                        sample, 
                        winning_sample, 
                        fraction, 
                        parent_data
                    )

                    print("New starting KS: {}".format(old_ks))

else:
    # Worker processes execute code below
    name = MPI.Get_processor_name()

    while True:
        comm.send(None, dest=0, tag=tags.READY)
        workunit = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        # unpack workunit
        master_data = workunit[0]
        parent_data = workunit[1]
        sample = workunit[2]
        weather_variables = workunit[3]
        total_ks = workunit[4]
        old_ks = workunit[5]
        fraction = workunit[6]
        sample_round = workunit[7]
        
        if tag == tags.START:

            # Do the work here
            while old_ks >= total_ks:
                parent_data, sample = replace_sample_subset(sample, fraction, parent_data)
                total_ks = cal_KS_total_statistic(
                    master_data,
                    sample, 
                    weather_variables
                )

            winning_sample = sample
            old_ks = total_ks

            # assemble result
            result = []
            result.append(parent_data)
            result.append(winning_sample)
            result.append(old_ks)
            result.append(sample_round)

            # send result to headnode   
            comm.send(result, dest=0, tag=tags.DONE)

            # send ready for new workunit
            #comm.send(result, dest=0, tag=tags.READY)
