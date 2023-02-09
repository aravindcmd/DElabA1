# import matplotlib.pyplot as plt
import numpy as np

# X, Y = np.loadtxt('firstlettercount.txt', delimiter=',', unpack=True)

# plt.bar(X, Y)
# plt.title('Line Graph using NUMPY')
# plt.xlabel('X')
# plt.ylabel('Y')
# plt.show()
from matplotlib import pyplot as plt
plt.rcParams["figure.figsize"] = [7,4]
plt.rcParams["figure.autolayout"] = True
bar_names = []
bar_heights = []
for line in open("part-r-00000", "r"):
   bar_name, bar_height = line.split()
   bar_names.append(bar_name)
   bar_heights.append(int(bar_height))

dictA = dict(zip(bar_names, bar_heights))
# dictA = sorted(dictA.items(), key=lambda x:x[1])
lists = sorted(dictA.items())
print(lists)
x, y = zip(*lists)
plt.bar(x, y)
plt.show()
