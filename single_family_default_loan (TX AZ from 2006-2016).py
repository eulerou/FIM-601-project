from dask import dataframe as dd
import numpy as np
from datetime import datetime

colname = ["POOL_ID", "LOAN_ID", "ACT_PERIOD", "CHANNEL", "SELLER", "SERVICER",
                        "MASTER_SERVICER", "ORIG_RATE", "CURR_RATE", "ORIG_UPB", "ISSUANCE_UPB",
                        "CURRENT_UPB", "ORIG_TERM", "ORIG_DATE", "FIRST_PAY", "LOAN_AGE",
                        "REM_MONTHS", "ADJ_REM_MONTHS", "MATR_DT", "OLTV", "OCLTV",
                        "NUM_BO", "DTI", "CSCORE_B", "CSCORE_C", "FIRST_FLAG", "PURPOSE",
                        "PROP", "NO_UNITS", "OCC_STAT", "STATE", "MSA", "ZIP", "MI_PCT",
                        "PRODUCT", "PPMT_FLG", "IO", "FIRST_PAY_IO", "MNTHS_TO_AMTZ_IO",
                        "DLQ_STATUS", "PMT_HISTORY", "MOD_FLAG", "MI_CANCEL_FLAG", "Zero_Bal_Code",
                        "ZB_DTE", "LAST_UPB", "RPRCH_DTE", "CURR_SCHD_PRNCPL", "TOT_SCHD_PRNCPL",
                        "UNSCHD_PRNCPL_CURR", "LAST_PAID_INSTALLMENT_DATE", "FORECLOSURE_DATE",
                        "DISPOSITION_DATE", "FORECLOSURE_COSTS", "PROPERTY_PRESERVATION_AND_REPAIR_COSTS",
                        "ASSET_RECOVERY_COSTS", "MISCELLANEOUS_HOLDING_EXPENSES_AND_CREDITS",
                        "ASSOCIATED_TAXES_FOR_HOLDING_PROPERTY", "NET_SALES_PROCEEDS",
                        "CREDIT_ENHANCEMENT_PROCEEDS", "REPURCHASES_MAKE_WHOLE_PROCEEDS",
                        "OTHER_FORECLOSURE_PROCEEDS", "NON_INTEREST_BEARING_UPB", "PRINCIPAL_FORGIVENESS_AMOUNT",
                        "ORIGINAL_LIST_START_DATE", "ORIGINAL_LIST_PRICE", "CURRENT_LIST_START_DATE",
                        "CURRENT_LIST_PRICE", "ISSUE_SCOREB", "ISSUE_SCOREC", "CURR_SCOREB",
                        "CURR_SCOREC", "MI_TYPE", "SERV_IND", "CURRENT_PERIOD_MODIFICATION_LOSS_AMOUNT",
                        "CUMULATIVE_MODIFICATION_LOSS_AMOUNT", "CURRENT_PERIOD_CREDIT_EVENT_NET_GAIN_OR_LOSS",
                        "CUMULATIVE_CREDIT_EVENT_NET_GAIN_OR_LOSS", "HOMEREADY_PROGRAM_INDICATOR",
                        "FORECLOSURE_PRINCIPAL_WRITE_OFF_AMOUNT", "RELOCATION_MORTGAGE_INDICATOR",
                        "ZERO_BALANCE_CODE_CHANGE_DATE", "LOAN_HOLDBACK_INDICATOR", "LOAN_HOLDBACK_EFFECTIVE_DATE",
                        "DELINQUENT_ACCRUED_INTEREST", "PROPERTY_INSPECTION_WAIVER_INDICATOR",
                        "HIGH_BALANCE_LOAN_INDICATOR", "ARM_5_YR_INDICATOR", "ARM_PRODUCT_TYPE",
                        "MONTHS_UNTIL_FIRST_PAYMENT_RESET", "MONTHS_BETWEEN_SUBSEQUENT_PAYMENT_RESET",
                        "INTEREST_RATE_CHANGE_DATE", "PAYMENT_CHANGE_DATE", "ARM_INDEX",
                        "ARM_CAP_STRUCTURE", "INITIAL_INTEREST_RATE_CAP", "PERIODIC_INTEREST_RATE_CAP",
                        "LIFETIME_INTEREST_RATE_CAP", "MARGIN", "BALLOON_INDICATOR",
                        "PLAN_NUMBER", "FORBEARANCE_INDICATOR", "HIGH_LOAN_TO_VALUE_HLTV_REFINANCE_OPTION_INDICATOR",
                        "DEAL_NAME", "RE_PROCS_FLAG", "ADR_TYPE", "ADR_COUNT", "ADR_UPB"]

column_names = {n:name for (n,name) in enumerate(colname)}

typename = ["str", "str", "str", "str", "str", "str",
                          "str", "float64", "float64", "float64", "float64",
                          "float64", "float64", "str", "str", "float64", "float64",
                          "float64", "str", "float64", "float64", "str", "float64",
                          "float64", "float64", "str", "str", "str",
                          "float64", "str", "str", "str", "str",
                          "float64", "str", "str", "str", "str",
                          "float64", "str", "str", "str", "str",
                          "str", "str", "float64", "str", "float64",
                          "float64", "float64", "str", "str", "str",
                          "float64", "float64", "float64", "float64", "float64", "float64",
                          "float64", "float64", "float64", "float64", "float64", "str",
                          "float64", "str", "float64", "float64", "float64", "float64",
                          "float64", "float64", "str", "float64", "float64", "float64",
                          "float64", "str", "float64", "str", "float64", "str",
                          "float64", "float64", "str", "str", "float64", "float64",
                          "float64", "float64", "float64", "float64", "float64", "float64",
                          "float64", "float64", "float64", "float64", "float64", "str",
                          "str", "str", "str", "str","str", "float64", "float64"]
types ={c:t for (c,t) in zip(colname,typename)}

# read all the loans from 2006 to 2016
df = dd.read_csv('E:/FNMA data/FNMA data/20*.csv', names=colname, dtype=types, delimiter='|')
# select loans in Texas and Arizona
df = df[(df.STATE=='TX')|(df.STATE=='AZ')]

relevant = [1,2,7,8,11,13,15,19,20,22,23,24,26,27,33,40,43,45,50,51,52,53,54,55,56,57,58,59,60,61,62,63]
# print({n:name for (n,name) in column_names.items() if n in relevant})
df = df.iloc[:,relevant]

# make sure current rate is available at disposition
df['CURR_RATE'] = df.CURR_RATE.fillna(method='ffill')
# loan age at foreclosure
df['LOAN_AGE'] = df.LOAN_AGE.fillna(method='ffill')+1

# select rows which goes into disposition
df = df[df.Zero_Bal_Code.isin(['02','03','09','15'])]

# fill NaN with 0 for each disposition related column
for col in df.columns[-11:]:
    df[col] = df[col].fillna(0)
    
# if last_upb is NaN, use current_upb
df['LAST_UPB'] = df['LAST_UPB'].mask(df.LAST_UPB.isna(), df.CURRENT_UPB)

# define the last activity date to be the disposition date, or the last act_period if disp_date is NaN
df['LAST_ACTIVITY_DATE'] = df['DISPOSITION_DATE'].mask(df.DISPOSITION_DATE.isna(), df.ACT_PERIOD)

# represent the date of origination, last paid installment and last activity as the number: 12*(year % 100) + month (e.g. 32016 -> 16*12+3, 112015 -> 15*12+11)
df['ORIG'] = df.ORIG_DATE.map(lambda x: float(x)%100, meta=('ORIG_DATE',float))*12\
           + df.ORIG_DATE.map(lambda x: (float(x)-float(x)%10000)/10000, meta=('ORIG_DATE',float))
df['LPI'] = df.LAST_PAID_INSTALLMENT_DATE.map(lambda x: float(x)%100, meta=('LAST_PAID_INSTALLMENT_DATE',float))*12\
           + df.LAST_PAID_INSTALLMENT_DATE.map(lambda x: (float(x)-float(x)%10000)/10000, meta=('LAST_PAID_INSTALLMENT_DATE',float))
df['LACT'] = df.LAST_ACTIVITY_DATE.map(lambda x: float(x)%100, meta=('LAST_ACTIVITY_DATE',float))*12\
           + df.LAST_ACTIVITY_DATE.map(lambda x: (float(x)-float(x)%10000)/10000, meta=('LAST_ACTIVITY_DATE',float))
# months from last paid installment date to disposition date
df['LPI2DISP'] = df.LACT-df.LPI

# accrued delinquent interest 
df['ACCRUED_INTEREST'] = (df.LAST_UPB - df.NON_INTEREST_BEARING_UPB)*(df.CURR_RATE/100-0.0035)*(df.LPI2DISP/12)
# net proceeds from disposition
df['DISP_NET_PROCEEDS'] = (df.NET_SALES_PROCEEDS + df.CREDIT_ENHANCEMENT_PROCEEDS + df.REPURCHASES_MAKE_WHOLE_PROCEEDS + df.OTHER_FORECLOSURE_PROCEEDS)\
                        - (df.FORECLOSURE_COSTS + df.PROPERTY_PRESERVATION_AND_REPAIR_COSTS + df.ASSET_RECOVERY_COSTS + df.MISCELLANEOUS_HOLDING_EXPENSES_AND_CREDITS + df.PRINCIPAL_FORGIVENESS_AMOUNT)

# calculate the net loss
df['NET_LOSS'] = df.LAST_UPB + df.ACCRUED_INTEREST - df['DISP_NET_PROCEEDS'] # notice that some of the loan have negative net loss
# if the disposition has not been completed (so that proceeds and cost entries are all NaNs), set net loss to be np.nan. 
df['NET_LOSS'] = df['NET_LOSS'].mask(df['DISP_NET_PROCEEDS']==0, np.nan)
# divide net loss by upb at foreclosure to obtain loss severity
df['LOSS_SEVERITY'] = df.NET_LOSS/df.LAST_UPB

df = df.drop(['ACT_PERIOD','CURRENT_UPB', 'PMT_HISTORY'], axis=1)

# df.to_csv('E:/FNMA data/FNMA data/default_final.csv',single_file=True)
