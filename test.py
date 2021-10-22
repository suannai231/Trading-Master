import numpy as np
import pandas as pd
from pandas import Series,DataFrame

# df = DataFrame()
# index = ['alpha','beta','gamma','delta','eta']
# for i in range(5):
#     a = DataFrame([np.linspace(i,5*i,5)],index=[index[i]])
#     df = pd.concat([df,a],axis=0)
#     print(df)
#     print('')

# df = pd.DataFrame({'Animal': ['Falcon', 'Falcon','Parrot', 'Parrot'],'Max Speed': [380., 370., 24., 26.]})
# print(df.groupby(['Animal']))

class boy:
    gender='male'
    interest='girl'
    def say(self):
        return 'i am a boy'

Jack = boy()
Jack.say()