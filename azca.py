import pandas as pd

data = pd.read_csv('E:/FNMA data/FNMA data/default_allstates.csv').iloc[:,1:]
data = data[data.STATE.isin(['AZ','CA'])]

# Converting datatype of loss to float.
data['LOSS_SEVERITY'] = pd.to_numeric(data['LOSS_SEVERITY'],errors='coerce')

# Define FICO column as the maximum of borrower FICO and coborrower FICO (NaN if both are NaN)
data['FICO'] = data['CSCORE_B'].mask(data['CSCORE_B'].isna(), data['CSCORE_C'])
data.drop(['CSCORE_B','CSCORE_C'],axis=1, inplace=True)

# remove loans with zero foreclosure proceeds or proceeds < 1000
proceeds = data['NET_SALES_PROCEEDS']+data['CREDIT_ENHANCEMENT_PROCEEDS']+data['REPURCHASES_MAKE_WHOLE_PROCEEDS']+data['OTHER_FORECLOSURE_PROCEEDS']
# len(data[proceeds<1000])/len(data)
data = data[proceeds>=1000]

# remove loans where last payment date is after disposition date (data error)
data = data[data['LPI2DISP']>=0]
# remove loans where liquidation takes more than 3 years
data = data[data['LPI2DISP']<42]

# set loss severity to be 1 if it's greater than 1 (when foreclosure proceeds > cost)
data['LOSS_SEVERITY']=data['LOSS_SEVERITY'].mask(data['LOSS_SEVERITY']>1,1)
# set loss severity to be 0 if it's negative (when foreclosure proceeds > cost + upb at default)
data['LOSS_SEVERITY']=data['LOSS_SEVERITY'].mask(data['LOSS_SEVERITY']<0,0)

# convert Categorical data into Numerical Data
data = pd.get_dummies(data, columns = ['PURPOSE'])
data = pd.get_dummies(data, columns = ['PROP'])
data = pd.get_dummies(data, columns = ['Zero_Bal_Code'])

# Moving the Target Variable to the end.
Target_var = data['LOSS_SEVERITY']
data.drop('LOSS_SEVERITY', axis=1, inplace=True)
data.insert(46, 'LOSS__SEVERITY', Target_var)

# data.to_csv("C:/Users/Euler Ou/Dropbox/NCSU/2021 Spring/FIM 601/project/data/lgd_azca.csv")
