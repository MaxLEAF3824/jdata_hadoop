# -*- coding: utf-8 -*
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

data_path = '../../../Data/RES/part-r-00000'
data = []
with open(data_path, 'r') as f:
    raw_data = f.readlines()
    # product id, sell num, scan num
    for rd in raw_data:
        temp_d = rd.split('\n')[0].split(',')
        data.append(temp_d)

data = np.array(data).astype(np.int)

# scatter
size = np.exp(data[:,1]*4)
sns.set_style("whitegrid")
fig =sns.scatterplot(x=data[:,1], y=data[:,2],\
    alpha=0.6,size=size,legend=False,linewidth=0)
fig.set_xlabel(u'Sales Volume')
fig.set_ylabel(u'Scan Frequency')
# plt.show()
scatter_fig = fig.get_figure()
scatter_fig.savefig('Sales-Scans.png', dpi = 400, bbox_inches='tight')