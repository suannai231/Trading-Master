import numpy as np

a=np.arange(15).reshape(3,5)

print(a)

print(np.mean(a,axis=0))
print(np.mean(a,axis=1))