def HHV(df,wr_days):
    high = 0
    if len(df) < wr_days:
        wr_days = df.index[-1]
    for i in range(len(df)-wr_days,len(df)):
        if df.loc[i,'high'] > high:
            high = df.loc[i,'high'] 
    return high

def LLV(df,wr_days):
    low = HHV(df,wr_days)
    if len(df) < wr_days:
        wr_days = df.index[-1]
    for i in range(len(df)-wr_days,len(df)):
        if df.loc[i,'low'] < low:
            low = df.loc[i,'low']
    return low

def Cal_WR(df,i,wr_days):
    if df.empty:
        return df
    close = df.loc[i,'close']
    high_value = HHV(df.loc[0:i],wr_days)
    df.loc[i,str(wr_days)+'days_high_value'] = high_value
    low_value = LLV(df.loc[0:i],wr_days)
    df.loc[i,str(wr_days)+'days_low_value'] = low_value
    if high_value == low_value:
        wr_value = 100
    else:
        wr_value = (high_value - close)/(high_value-low_value)*100
    df.loc[i,'WR'+str(wr_days)] = wr_value
    return df

def Cal_Hist_WR(df,wr_days):
    if df.empty:
        return df
    last = len(df) - 1
    df = Cal_WR(df, last, wr_days)
    current_index = len(df)-2
    while current_index>=0:
        remove_index = current_index+1
        add_index = current_index-wr_days
        if (df.loc[remove_index,'high'] == df.loc[remove_index,str(wr_days)+'days_high_value']) | (df.loc[remove_index,'low'] == df.loc[remove_index,str(wr_days)+'days_low_value']):
            df = Cal_WR(df, current_index ,wr_days)
            high_value = df.loc[current_index,str(wr_days)+'days_high_value']
            low_value = df.loc[current_index,str(wr_days)+'days_low_value']
        else:
            high_value = df.loc[remove_index,str(wr_days)+'days_high_value']
            low_value = df.loc[remove_index,str(wr_days)+'days_low_value']

        if(add_index>=0):
            if df.loc[add_index,'high'] > high_value:
                high_value = df.loc[add_index,'high']
            if df.loc[add_index,'low'] < low_value:
                low_value = df.loc[add_index,'low']

        df.loc[current_index,str(wr_days)+'days_high_value'] = high_value
        df.loc[current_index,str(wr_days)+'days_low_value'] = low_value
        if high_value == low_value:
            wr_value = 100
        else:
            wr_value = (high_value - df.loc[current_index,'close'])/(high_value-low_value)*100
            df.loc[current_index,'WR'+str(wr_days)] = wr_value
        current_index-=1
    return df