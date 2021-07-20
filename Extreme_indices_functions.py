# Extreme indices functions

# count of number of days Tmin (ds) is less than 0C
def frostdays(ds_Tmin, time_group):

    """ Extreme index: Frost Days - count of number of days Tmin (ds) is less than 0C.
        
        Args:
        ds_Tmin (xarray): data set of minimum temperature (Tmin)
        time_group (string): group data by time_group (e.g. 'M', 'Y') 
    """
    count_FD = ds_Tmin.where(ds_Tmin<0).resample(time = time_group).count(dim = 'time')  

    return count_FD


# count of number of days when Tmax is greater than 25C
def summerdays(ds_Tmax, time_group):
    
    """ Extreme index: Summer Days - count of number of days Tmax (ds) is greater than 25C.
        
        Args:
        ds_Tmax (xarray): data set of maximum temperature (Tmax)
        time_group (string): group data by time_group (e.g. 'M', 'Y')
    """
    count_SU = ds_Tmax.where(ds_Tmax>25).resample(time = time_group).count(dim = 'time')

    return count_SU


# count of number of days Tmax (ds) is less than 0C
def icingdays(ds_Tmax, time_group):

    """ Extreme index: Icing Days - count of number of days Tmax (ds) is less than 0C.
        
        Args:
        ds_Tmax (xarray): data set of maximum temperature (Tmax)
        time_group (string): group data by time_group (e.g. 'M', 'Y')

    """
    count_ID = ds_Tmax.where(ds_Tmax<0).resample(time = time_group).count(dim = 'time')

    return count_ID


# count of number of days Tmin (ds) is greater than 20C
def tropicalnights(ds_Tmin, time_group):

    """ Extreme index: Tropical Nights - count of number of days Tmin (ds) is greater than 20C.
        
        Args:
        ds_Tmin (xarray): data set of minimum temperature (Tmin)
        time_group (string): group data by time_group (e.g. 'M', 'Y') 
    """
    count_TR = ds_Tmin.where(ds_Tmin>20).resample(time = time_group).count(dim = 'time')

    return count_TR


# maximum daily maximum temperature each month 
def T_maxmax(ds_Tmax, time_group):

    """ Extreme index: TXx - maximum of daily maximum temperature each month
        
        Args:
        ds_Tmax (xarray): data set of maximum temperature (Tmax)
        time_group (string): group data by time_group (e.g. 'M') 
    """
    #group data by month - find max each month
    TXx = ds_Tmax.resample(time=time_group).max(dim='time')

    return TXx


# maximum daily minimum temperature each month 
def T_maxmin(ds_Tmin, time_group):    

    """ Extreme index: TNx - maximum daily minimum temperature each month 
        
        Args:
        ds_Tmin (xarray): data set of minimum temperature (Tmin)
        time_group (string): group data by time_group (e.g. 'M') 
    """
    #group data by month - find max each month
    TNx =  ds_Tmin.resample(time=time_group).max(dim='time')

    return TNx


# minimum daily minimum temperature each month 
def T_minmin(ds_Tmin, time_group):
    """ Extreme index: TNn - minimum daily minimum temperature each month 
        
        Args:
        ds_Tmin (xarray): data set of minimum temperature (Tmin)
        time_group (string): group data by time_group (e.g. 'M') 
    """
    #group data by month - find min each month
    TNn = ds_Tmin.resample(time=time_group).min(dim='time')

    return TNn


# minimum daily maximum temperature each month 
def T_minmax(ds_Tmax, time_group):

    """ Extreme index: TXn - minimum of daily maximum temperature each month
        
        Args:
        ds_Tmax (xarray): data set of maximum temperature (Tmax)
        time_group (string): group data by time_group (e.g. 'M') 
    """    
    #group data by month - find min each month
    TXn = ds_Tmax.resample(time=time_group).min(dim='time')

    return TXn


# Percentage of days when TN or TX < 10th percentile 
def T_10p(dataset, time_group, start_date, end_date):
    """ Extreme index: TN10p/TX10p - percentage of days when TN or TX < 10th percentile
        
        Args:
        dataset (xarray): data set of either Tmin or Tmax
        time_group (string): group data by time_group (e.g. 'M')
        start_date (string): start date of period over which to calculate percentile
        end_date (string): end date of period over which to calculate percentile
    """    

    # find the 10th percentile
    p10 = dataset.sel(time=slice(start_date, end_date)).quantile(0.1, dim=['time'])
    # group data by month - find min each month
    T_10p_count = dataset.where(dataset < p10).resample(time = time_group).count(dim = 'time')
    
    # find where any NaN values occur
    NaN_ds = dataset.isnull() # listed as True if it's a NaN value
    # count the number of NaN values per month 
    NaN_count = NaN_ds.where(NaN_ds==True).resample(time = time_group).count(dim = 'time')
    
    # find the no. days in each month (remembering to minus any days that have NaN values)
    mon_range = T_10p_count.time.dt.days_in_month - NaN_count

    # convert the (monthly) count of TN10p to a percentage
    T_10p = T_10p_count*100/(mon_range)  
    
    # remove the quantile dimension so I can combine datasets (ie T_90p) later
    del T_10p['quantile']

    return T_10p


# Percentage of days when TN or TX > 90th percentile 
def T_90p(dataset, time_group, start_date, end_date):
    """ Extreme index: TN90p/TX90p - percentage of days when TN or TX > 90th percentile
        
        Args:
        dataset (xarray): data set of either Tmin or Tmax
        time_group (string): group data by time_group (e.g. 'M')
        start_date (string): start date of period over which to calculate percentile
        end_date (string): end date of period over which to calculate percentile
    """    

    # find the 10th percentile
    p90 = dataset.sel(time=slice(start_date, end_date)).quantile(0.9, dim=['time'])

    # group data by month - find min each month
    T_90p_count = dataset.where(dataset > p90).resample(time = time_group).count(dim = 'time')

    # find where any NaN values occur
    NaN_ds = dataset.isnull() # listed as True if it's a NaN value
    # count the number of NaN values per month 
    NaN_count = NaN_ds.where(NaN_ds==True).resample(time = time_group).count(dim = 'time')

    # find the no. days in each month (remembering to minus any days that have NaN values)
    mon_range = T_90p_count.time.dt.days_in_month - NaN_count

    # convert the (monthly) count of TN10p to a percentage
    T_90p = T_90p_count*100/(mon_range)
    
    # remove the quantile dimension so I can combine datasets (ie T_10p) later
    del T_90p['quantile']

    return T_90p


# daily temperature range 
def daily_range(ds_Tmin, ds_Tmax, time_group):

    """ Extreme index: DTR - average daily temperature range for each month (1 value per month of each year)
       
        Args:
        ds_Tmin (xarray): data set of minimum temperature (Tmax)
        ds_Tmax (xarray): data set of maximum temperature (Tmax)
        time_group (string): group data by time_group (e.g. 'M') 
    """      
    DTR = (ds_Tmax - ds_Tmin).resample(time=time_group).mean(dim='time')

    return DTR    


# extreme temperature range 
def extreme_range(ds_TNn, ds_TXx):
    
    """ Extreme index: ETR - extreme temperature range for each month (1 value per month of each year)
        
        Args:
        ds_TNn (xarray): data set of minimum temperature (Tmax)
        ds_TXx (xarray): data set of maximum temperature (Tmax)
        time_group (string): group data by time_group (e.g. 'M') 
    """  
    ETR = ds_TXx - ds_TNn
    
    return ETR


# function to calculate all extreme indices and put them in an xarray
def extreme_indices(dataset, time_group, start_date, end_date):
    
    """ Extreme indices: Calculate selected temperature extreme indices and store them in an xarray. 
        
        Args:
        dataset (xarray): data set of temperature  containing both Tmin and Tmax
        time_group (string): group data by time_group (e.g. 'M') 
        start_date (string): start date of period over which to calculate percentile
        end_date (string): end date of period over which to calculate percentile
    """ 
    
    import xarray as xr 
    
    # select out Tmin and Tmax from input dataset
    ds_Tmin = dataset.Tmin
    ds_Tmax = dataset.Tmax
    
    # calculate all the extreme indices needed
    FD = frostdays(ds_Tmin, time_group)
    SU = summerdays(ds_Tmax, time_group)
    ID = icingdays(ds_Tmax, time_group)
    TR = tropicalnights(ds_Tmin, time_group)
    TXx = T_maxmax(ds_Tmax, time_group)
    TNx = T_maxmin(ds_Tmin, time_group)
    TNn = T_minmin(ds_Tmin, time_group)
    TXn = T_minmax(ds_Tmax, time_group)
    TN10p = T_10p(ds_Tmin, time_group, start_date, end_date)
    TX10p = T_10p(ds_Tmax, time_group, start_date, end_date)
    TN90p = T_90p(ds_Tmin, time_group, start_date, end_date)
    TX90p = T_90p(ds_Tmax, time_group, start_date, end_date)
    DTR = daily_range(ds_Tmin, ds_Tmax, time_group)
    ETR = extreme_range(TNn, TXx)
    
    # put all indicies into one xarray
    indicies = xr.Dataset({'FD': FD, 'SU': SU, 'ID': ID, 'TR': TR, 'TXx': TXx, 'TNx': TNx, 'TNn': TNn, 'TXn': TXn, 'TN10p': TN10p, 'TX10p': TX10p, 'TN90p': TN90p, 'TX90p': TX90p, 'DTR': DTR, 'ETR': ETR})
    
    return indicies
                    





