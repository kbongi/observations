# functions used across models, reanalysis and obs sections 

# define function to calculate monthly anomalies for a multidimensional array of models
def monthly_anomaly(dataset, start_date, end_date):
    
    """ Calculate monthly anomalies for a multidimensional array of models.  
        
        Args:
        dataset (xarray): data set of climate variable (e.g tas)
        start_date (date_str): start date of climatology to calculate monthly anomaly
        end_date (date_str): end date of climatology to calculate monthly anomaly
    """
    
    # group the data into months
    variable_monthly = dataset.groupby('time.month')

    # calculate the mean climatology along each month for the time period 1850-1900 
    clim_monthly = dataset.sel(time = slice(f'{start_date}', f'{end_date}')).groupby('time.month').mean(dim = 'time')

    # caclulate the anomalies for each month and return it as an array
    multi_monthly_anom = (variable_monthly - clim_monthly)

    return multi_monthly_anom

# define function to calculate the seasonal mean used in seasonal anomaly calculation:
def seasonal_mean(data):
    """ Calculate the seasonal mean used in seasonal anomaly calculation.  
        
        Args:
        data (xarray): data set of climate variable (e.g tas)
    """
    return data.groupby('time.season').mean()


# function to calculate a seasonal anomaly for a multidimensional xarray over a time period entered by user
def seasonal_anomaly(dataset, start_date, end_date):
    """ Calculate a seasonal anomaly for a multidimensional xarray over a time period entered by user.  
        
        Args:
        dataset (xarray): data set of climate variable (e.g tas)
        start_date (date_str): start date to calculate seasonal anomaly
        end_date (date_str): end date to calculate seasonal anomaly
    """
    # first I need to define a new coordinate (seasonyear) so that december gets counted with the adjoining jan and feb
    seasonyear = (dataset.time.dt.year + (dataset.time.dt.month//12)) 
    dataset.coords['seasonyear'] = seasonyear
    
        
    # group data into seasons and calculate the seasonal mean for each year in the dataset 
    yearly_seasonal = dataset.groupby('seasonyear').apply(seasonal_mean)

    # calculate the mean climatology along each season for the time period 
    clim_seasonal = yearly_seasonal.sel(seasonyear = slice(f'{start_date}',f'{end_date}')).mean(dim = 'seasonyear')

    # calculate the anomaly and returns it as an xarray
    multi_seasonal_anom = (yearly_seasonal - clim_seasonal)
        
    return multi_seasonal_anom

# function to group the data by season
def seasonal_group(dataset):
    """ Calculate a seasonal anomaly for a multidimensional xarray over a time period entered by user.  
        
        Args:
        dataset (xarray): data set of climate variable (e.g tas)
    """
    # first I need to define a new coordinate (seasonyear) so that december gets counted with the adjoining jan and feb
    seasonyear = (dataset.time.dt.year + (dataset.time.dt.month//12)) 
    dataset.coords['seasonyear'] = seasonyear
    
        
    # group data into seasons and calculate the seasonal mean for each year in the dataset 
    yearly_seasonal = dataset.groupby('seasonyear').apply(seasonal_mean)
    
    return yearly_seasonal

# defines an array of titles for seasonal spatial graphs
def seasonal_title(K_dates, season_name, season):
    """Create titles for graphs by combining strings for each year, season post-eruption.  
    
    Args:
        K_dates (list): list of years to be plotted
        title_label (list): list of summer relative to eruption
        season_name(list): season name (e.g. 'summer')
        season (list): season (e.g. 'DJF')
    """
    title_label = [f'{season_name} prior to eruption, ', f'1st {season_name} post-eruption, ', 
                   f'2nd {season_name} post-eruption, ', f'3rd {season_name} post-eruption, ']
    
    titles=[]
    for i,vals in enumerate(K_dates):
        t = title_label[i] + season + f' {K_dates[i]}'
        titles.append(t)
    
    return titles

# calculate the seasonal cycle 
def seasonal_amp(dataset):
    """Calculate the amplitude of the seasonal cycle (maixmum - minimum) for each year. 
        Args:
        dataset (xarray): subyearly data set of temp/rainfall/other
    """
    if hasattr(dataset, 'time'):
        # find the max monthly value for each year
        smax = dataset.groupby('time.year').max(dim='time')
        # find the min monthly value for each year
        smin = dataset.groupby('time.year').min(dim='time')
        
    elif hasattr(dataset, 'seasonyear'):
        # find the max seasonal value for each year
        smax = dataset.groupby('seasonyear').max(dim='season')
        # find the min seasonal value for each year
        smin = dataset.groupby('seasonyear').min(dim='season')
        
    # find the amplitude of the seasonal cycle (max-min) for each year 
    seasonal_cycle_amp = smax-smin
    
    return seasonal_cycle_amp


# calculate the nino 3.4 index 
def nino34(sst_dataset, start_date, end_date, std):
    """ Calculate the NINO34 index from SST values and normalise by dividing by the standard deviation calculate over user specified time period.   
        
        Args:
        sst_dataset (xarray): data set of sea surface temperature values
        start_date (date_str): start date of std climatology
        end_date (date_str): end date of std climatology
        std (int): if std==1, calculate the std and divide NINO34 index by std
    """
    # select out the region for nino34 definition
    region = sst_dataset.sel(lat=slice(-5,5), lon=slice(190,240))
    
    # calculate the mean climatology along each month
    clim = region.sel(time = slice(f'{start_date}', f'{end_date}')).groupby('time.month').mean(dim = ['time','lat','lon'])
    
    # calculate the anomaly using input dates for climatology and take the lat lon mean 
    #anom = monthly_anom_xr(region, f'{start_date}', f'{start_date}').mean(dim=['lat','lon'])
    anom = (region.groupby('time.month') - clim).mean(dim=['lat','lon'])
    
    # chunk the data into groups of 5 timepoints so I can then use rolling mean 
    anom = anom.chunk({'time': 5})
    
    if std == 1:
        # calculate the standard deviation so we can normalise the model data 
        std = region.sel(time = slice(f'{start_date}', f'{end_date}')).mean(dim=['lat', 'lon']).std(dim = ['time'])
        
        # calculate the nino3.4 index using a rolling 5 month mean and normalised by the std
        nino34_index = anom.rolling(time=5).mean() / std
    elif std == 0:
            nino34_index = anom.rolling(time=5).mean()
    
    return nino34_index
    

