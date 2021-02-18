from dask import dataframe as dd
import os

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

# the index of some preliminary columns that we will use later
# check the glossary file and check "Date Bound Notes" and "Single-Family (SF) Loan Performance" columns to remove columns where data are unavailable
# then check the description column to see whether the rest of them are useful
relevant = [i-1 for i in [2,3,4,5,6,8,9,
                          10,12,13,16,17,18,20,21,23,24,25,26,27,28,
                          30,31,32,35,36,
                          40,41,42,44,46,
                          54,55,56,57,58,59,
                          60,61,62,63,
                          73,74,
                          80,81]]
# print({n:name for (n,name) in column_names.items() if n in relevant})

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

# some potential filters that will be used later
(dask_df.SELLER=='Wells Fargo Bank, N.A.')

# if you save all the quaterly datas in a folder, use this to retrieve a list of file names
# files = []
# with os.scandir('E:\FNMA data\FNMA data') as entries:
#     for file in entries:
#         files.append(file)
        
# or we can simply read multiple csv files as follows:
dask_df = dd.read_csv('E:/FNMA data/FNMA data/2006*.csv', names=colname, dtype=types, delimiter='l') # to read 2006 data
# dask_df = dd.read_csv('E:/FNMA data/FNMA data/20*.csv', names=colname, dtype=types, delimiter='l') # to read all the data
        
# read and filter our dataset
dask_df = dd.read_csv(filename, names=colname, dtype=types, delimiter='|')
dask_df = dask_df[(dask_df.STATE=='TX')|(dask_df.STATE=='AZ')]
dask_df = dask_df.iloc[:,relevant]
print(dask_df.head())

# this is to check if you are not sure if some column cantain no values at all
# print(dask_df.loc[:,'HIGH_BALANCE_LOAN_INDICATOR'].isna().sum().compute())

# export the filtered dataset
# dask_df = dd.to_csv(filename, single_file=TRUE)
