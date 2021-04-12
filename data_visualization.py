# In this file, we will look at the LGD distribution of subsets of our lgd_azca dataset.

import pandas as pd
import seaborn as sns

data = pd.read_csv('C:/Users/Euler Ou/Dropbox/NCSU/2021 Spring/FIM 601/project/data/lgd_azca.csv')
az = data[data.STATE=='AZ']
ca = data[data.STATE=='CA']

### LGV distribution for Arizona and California
ls_az = az['LOSS__SEVERITY']
ls_ca = ca['LOSS__SEVERITY']

print(len(ls_az), len(ls_ca))

fig,axes = plt.subplots(2,1,figsize=(10,10))

sns.histplot(ls_az, stat='probability',binwidth=0.025, binrange=(0,1.1), ax=axes[0])
axes[0].set_ylim((0,0.18))
sns.histplot(ls_ca,stat='probability',binwidth=0.025, binrange=(0,1.1), ax=axes[1])
axes[1].set_ylim((0,0.18))

plt.tight_layout()

# check the percentage of zero-loss loan 
print(len(ls_az[ls_az==0])/len(ls_az))
print(len(ls_ca[ls_ca==0])/len(ls_ca))

### Look at LGV for loans where original LTV == original CLTV or not
az_test = az[az.OLTV!=az.OCLTV]
az_test2 = az[az.OLTV==az.OCLTV]
ca_test = ca[ca.OLTV!=ca.OCLTV]
ca_test2 = ca[ca.OLTV==ca.OCLTV]

fig,axes = plt.subplots(4,1,figsize=(10,10))

sns.histplot(az_test['LOSS__SEVERITY'],stat='probability',binwidth=0.005,binrange=[0,1], ax=axes[0])
sns.histplot(az_test2['LOSS__SEVERITY'],stat='probability',binwidth=0.005,binrange=[0,1], ax=axes[1])
sns.histplot(ca_test['LOSS__SEVERITY'],stat='probability',binwidth=0.005,binrange=[0,1], ax=axes[2])
sns.histplot(ca_test2['LOSS__SEVERITY'],stat='probability',binwidth=0.005,binrange=[0,1], ax=axes[3])

# check the percentage of zero-loss loan 
print(len(az_test[az_test.LOSS__SEVERITY==0])/len(az_test))
print(len(az_test2[az_test2.LOSS__SEVERITY==0])/len(az_test2))
print(len(ca_test[ca_test.LOSS__SEVERITY==0])/len(ca_test))
print(len(az_test2[ca_test2.LOSS__SEVERITY==0])/len(ca_test2))

### check LTV for loans with different liquidation types
# Arizona
ls_az_2 = ca['LOSS__SEVERITY'][ca.Zero_Bal_Code_2==1]
ls_az_3 = ca['LOSS__SEVERITY'][ca.Zero_Bal_Code_3==1]
ls_az_9 = ca['LOSS__SEVERITY'][ca.Zero_Bal_Code_9==1]
ls_az_15 = ca['LOSS__SEVERITY'][ca.Zero_Bal_Code_15==1]

print(len(ca), len(ls_az_2), len(ls_az_3), len(ls_az_9), len(ls_az_15))

fig,axes = plt.subplots(5,1,figsize=(10,10))

sns.histplot(ls_az, stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[0])
sns.histplot(ls_az_2,stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[1])
sns.histplot(ls_az_3,stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[2])
sns.histplot(ls_az_9,stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[3])
sns.histplot(ls_az_15,stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[4])

plt.tight_layout()

# California
ls_ca_2 = ca['LOSS__SEVERITY'][ca.Zero_Bal_Code_2==1]
ls_ca_3 = ca['LOSS__SEVERITY'][ca.Zero_Bal_Code_3==1]
ls_ca_9 = ca['LOSS__SEVERITY'][ca.Zero_Bal_Code_9==1]
ls_ca_15 = ca['LOSS__SEVERITY'][ca.Zero_Bal_Code_15==1]

print(len(ca), len(ls_ca_2), len(ls_ca_3), len(ls_ca_9), len(ls_ca_15))

fig,axes = plt.subplots(5,1,figsize=(10,10))

sns.histplot(ls_ca, stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[0])
sns.histplot(ls_ca_2,stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[1])
sns.histplot(ls_ca_3,stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[2])
sns.histplot(ls_ca_9,stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[3])
sns.histplot(ls_ca_15,stat='probability',binwidth=0.05, binrange=(0,1.5), ax=axes[4])

ls_ca_DTI = ca['LOSS__SEVERITY'][ca['DTI'].notna()]
ls_ca_NON = ca['LOSS__SEVERITY'][ca['DTI'].isna()]

print(len(ca), len(ls_ca_DTI), len(ls_ca_NON),'\n')

fig,axes = plt.subplots(3,1,figsize=(10,10))

sns.histplot(ca['LOSS__SEVERITY'], stat='probability',binwidth=0.01, binrange=(0,1), ax=axes[0])
sns.histplot(ls_ca_DTI,stat='probability',binwidth=0.01, binrange=(0,1), ax=axes[1])
sns.histplot(ls_ca_NON,stat='probability',binwidth=0.01, binrange=(0,1), ax=axes[2])

plt.tight_layout()

# percentage of zero loss
print(len(ls_ca_DTI[ls_ca_DTI==0])/len(ls_ca_DTI))
print(len(ls_ca_NON[ls_ca_NON==0])/len(ls_ca_NON))
plt.tight_layout()

### Loans where DTI is missing or not

#arizona
ls_az_DTI = az['LOSS__SEVERITY'][az['DTI'].notna()]
ls_az_NON = az['LOSS__SEVERITY'][az['DTI'].isna()]

print(len(az), len(ls_az_DTI), len(ls_az_NON))

fig,axes = plt.subplots(3,1,figsize=(10,10))

sns.histplot(az['LOSS__SEVERITY'], stat='probability',binwidth=0.01, binrange=(0,1), ax=axes[0])
sns.histplot(ls_az_DTI,stat='probability',binwidth=0.01, binrange=(0,1), ax=axes[1])
sns.histplot(ls_az_NON,stat='probability',binwidth=0.01, binrange=(0,1), ax=axes[2])

plt.tight_layout()

# percentage of zero loss
print(len(ls_az_DTI[ls_az_DTI==0])/len(ls_az_DTI))
print(len(ls_az_NON[ls_az_NON==0])/len(ls_az_NON))


