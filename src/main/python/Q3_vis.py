# -*- coding: utf-8 -*
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def vis_and_statistic(data, split, axis):
    data = data[split]
    freq = data[:,1].astype(np.int)
    time = data[:,0]

    axesSub = sns.lineplot(data=freq, ax=axis)
    axesSub.set_xlabel(split)
    axesSub.set_ylabel('Frequency')
    axesSub.set_xticks([])  

    ids = np.argsort(-freq)
    # print("#########")
    # print(
    #     'Peaks in {} are {}'.format(split, time[ids[:3]])
    # )

if __name__ == '__main__':
    data_path = '../../../Data/User-Time/user-time/user-time_'
    data = {}
    split_interval = ['day','hour','min','sec']
    for sp in split_interval:
        data[sp] = []
        with open(data_path+sp, 'r') as f:
            raw_data = f.readlines()
            # time, sell num
            for rd in raw_data:
                temp_d = rd.split('\n')[0].split(',')
                data[sp].append(temp_d)

        data[sp] = np.array(data[sp])

    fig, ax =plt.subplots(2,2,constrained_layout=True, figsize=(12, 3))
    for i, sp in enumerate(split_interval):
        vis_and_statistic(data, sp, ax[i//2, i%2])

    plt.savefig('Frequency.png', dpi = 400, bbox_inches='tight')
    plt.show()

