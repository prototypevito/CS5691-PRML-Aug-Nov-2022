#!/usr/bin/env python
# coding: utf-8

# In[15]

import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
bookings_main=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\bookings.csv")
bookings_duplicate=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\bookings_data.csv")
customer_data=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\customer_data.csv")
hotels_data=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\hotels_data.csv")
payments_data=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\payments_data.csv")
test_data=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\sample_submission_5.csv")
train_data=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\train_data.csv")
#remove the unnecessary extra order statuses
bookings_main['booking_status'] = bookings_main['booking_status'].replace(['processed', 'canceled','unavailable','invoiced','processing','created','approved'], 'not_completed')
bookings_main["booking_create_timestamp"] = pd.to_datetime(bookings_main["booking_create_timestamp"])
bookings_main["booking_approved_at"] = pd.to_datetime(bookings_main["booking_approved_at"])
bookings_main["booking_checkin_customer_date"] = pd.to_datetime(bookings_main["booking_checkin_customer_date"])
main=bookings_main.copy()
a=bookings_main[bookings_main['booking_status']=='not_completed'][['booking_create_timestamp','booking_approved_at']]
b=a[a.booking_approved_at.notnull()]
c=a[a.booking_approved_at.isnull()]
b['diff']=b['booking_approved_at']-b['booking_create_timestamp']
mean_difference=b['diff'].mean()
main['booking_approved_at']=main['booking_approved_at'].fillna(main['booking_create_timestamp']+mean_difference)
main['approval_time_diff']=main['booking_approved_at']-main['booking_create_timestamp']
main['approval_time_diff_minutes']=(main['approval_time_diff'].dt.total_seconds()/60.0).round(2)
main['approval_time_diff_minutes'].mean()
main['booking_quarter']=main['booking_create_timestamp'].dt.quarter
main['booking_year']=main['booking_create_timestamp'].dt.year
main['booking_dayofweek']=main['booking_create_timestamp'].dt.dayofweek
main['booking_0to8'] = True
main.loc[main['booking_create_timestamp'].dt.hour>=8 , 'booking_0to8'] = False
main=main.drop(['approval_time_diff'],axis=1)
main[main['booking_checkin_customer_date'].isna()].head()
for year in [2006,2007,2008]:
    a=main[main['booking_year']==year]
    a['time_diff']=a['booking_checkin_customer_date']-a['booking_create_timestamp']
    main.loc[main['booking_year']==year,'booking_checkin_customer_date'] = main.loc[main['booking_year']==year,'booking_checkin_customer_date'].fillna(main.loc[main['booking_year']==year]['booking_create_timestamp']+a['time_diff'].mean())
main['checkin_month']=main['booking_checkin_customer_date'].dt.month
main['checkin_year']=main['booking_checkin_customer_date'].dt.year
main['checkin_time_diff']=((main['booking_checkin_customer_date']-main['booking_create_timestamp']).dt.total_seconds()/3600).round(2)
main.head()
bookings_main=main.copy()
customer_data=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\customer_data.csv")
#customer_data=customer_data[:-1]
customer_data['freq_count'] = customer_data.groupby('customer_unique_id')['customer_unique_id'].transform('count')
customer_data.loc[customer_data['freq_count'] ==2, 'customer_bookings_quantity'] = 2
customer_data.loc[customer_data['freq_count'] <= 1, 'customer_bookings_quantity'] = 1
customer_data.loc[customer_data['freq_count'] > 2, 'customer_bookings_quantity'] = 3
customer_data=customer_data.drop(['freq_count'],axis=1)
#customer_data.tail(1)
customer_data=customer_data.drop(['customer_unique_id'],axis=1)
#customer_data.info()
df2=customer_data.copy()
#df2.info()
df1=bookings_main.copy()
#df1.info()
df3=pd.merge(df1, df2, on='customer_id', how="left")
#df3.info()
bookings_main2=df3.copy()
#bookings_main2.head()
#bookings_main2.info()
#remove both customer unique id and customer id as customer id is same as booking id and is not used anywhere
bookings_main2=bookings_main2.drop(['customer_id'],axis=1)
#bookings_main2.info()
df31=bookings_duplicate.copy()
#df31.head()
#df31.info()
df31["booking_expiry_date"] = pd.to_datetime(df31["booking_expiry_date"])
df31['agent_fees'] = df31['agent_fees']/df31['price']
means = pd.DataFrame(df31.groupby('seller_agent_id').mean()['agent_fees'].round(2))
means=means.reset_index().rename(columns={'agent_fees':'agent_mean_fees'})
dummy=pd.merge(df31, means, on='seller_agent_id', how="left")
counts=pd.DataFrame(dummy.groupby('seller_agent_id').size())
counts.reset_index()
counts=counts.reset_index().rename(columns={0:'agent_count'})
dummy=pd.merge(dummy, counts, on='seller_agent_id', how="left")
counts_hotel=pd.DataFrame(dummy.groupby('hotel_id').size())
counts_hotel=counts_hotel.reset_index().rename(columns={0:'hotel_count'})
dummy=pd.merge(dummy, counts_hotel, on='hotel_id', how="left")
df31=dummy.copy()
df31['freq']=df31.groupby('booking_id').booking_sequence_id.max()[df31.booking_id].reset_index().booking_sequence_id
df31=df31.drop(['booking_sequence_id'],axis=1)
#df31.info()
hotels_data=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\hotels_data.csv")
hotels_data.fillna({'hotel_category':hotels_data['hotel_category'].mode()[0], 
                    'hotel_name_length': hotels_data['hotel_name_length'].mean(),
                    'hotel_description_length': hotels_data['hotel_description_length'].mean(),
                    'hotel_name_length': hotels_data['hotel_name_length'].mean(),
                    'hotel_photos_qty': hotels_data['hotel_photos_qty'].mean()}, inplace=True)
df32=pd.merge(df31, hotels_data, on='hotel_id', how="left")
dummy1=bookings_main2.copy()
dummy2=df32.copy()
dummy3=pd.merge(dummy1,dummy2,on='booking_id',how='left')
dummy3=dummy3.drop(['hotel_id','seller_agent_id'],axis=1)
dummy3.fillna({'price':dummy3['price'].mean(), 
               'hotel_category': dummy3['hotel_category'].mode()[0],
               'hotel_name_length': dummy3['hotel_name_length'].mode()[0],
               'hotel_description_length': dummy3['hotel_description_length'].mean(),
               'hotel_name_length': dummy3['hotel_name_length'].mean(),
               'hotel_photos_qty': dummy3['hotel_photos_qty'].mean(),
               'agent_fees': dummy3['agent_fees'].mean(),
               'agent_mean_fees' :dummy3['agent_mean_fees'].mean(),
               'agent_count' :dummy3['agent_count'].mean(),
               'hotel_count' :dummy3['hotel_count'].mean(),
               'freq': 1,
              }, inplace=True)
payments_data=pd.read_csv(r"C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\data\payments_data.csv")
payments_data['payment_freq']=payments_data.groupby('booking_id').payment_sequential.max()[payments_data.booking_id].reset_index().payment_sequential
total_payment=pd.DataFrame(payments_data.groupby('booking_id').sum()['payment_value'])
total_payment=total_payment.reset_index().rename(columns={'payment_value':'payment_total'})
dummy=pd.merge(payments_data, total_payment, on='booking_id', how="left")
total_installment=pd.DataFrame(payments_data.groupby('booking_id').sum()['payment_installments'])
total_installment=total_installment.reset_index().rename(columns={'payment_installments':'total_installments'})
dummy=pd.merge(dummy, total_installment, on='booking_id', how="left")
#max_type=pd.DataFrame(payments_data.groupby('booking_id')['payment_type'].agg(pd.Series.mode))
max_type=payments_data.groupby('booking_id',as_index=False).agg({'payment_type': lambda x:x.mode()[0]})
max_type=max_type.reset_index(drop=True).rename(columns={'payment_type':'mode_payment_type'})
dummy=pd.merge(dummy, max_type, on='booking_id', how="left")
dummy=dummy.drop(['payment_sequential', 'payment_type', 'payment_installments', 'payment_value'],axis=1)
dummy.drop_duplicates().reset_index(drop=True)
dummy4=pd.merge(dummy3,dummy,on='booking_id',how='left')
dummy4.fillna({'payment_total':dummy4['payment_total'].mean(), 
               'payment_freq': dummy4['payment_freq'].mode()[0],
               'total_installments': dummy4['total_installments'].mode()[0],
               'mode_payment_type': dummy4['mode_payment_type'].mode()[0],
              }, inplace=True)
b=dummy4[dummy4.booking_approved_at.notnull()]
b['diff']=(b['booking_expiry_date']-b['booking_create_timestamp'])
mean_difference=b['diff'].mean()
dummy4['booking_expiry_date']=dummy4['booking_expiry_date'].fillna(dummy4['booking_create_timestamp']+mean_difference)
dummy4['expiry_time_diff_days']=(dummy4['booking_expiry_date']-dummy4['booking_create_timestamp']).dt.days
dummy4.loc[dummy4['expiry_time_diff_days']>3671, 'expiry_days'] = 2
dummy4.loc[(dummy4['expiry_time_diff_days']>=3665) & (dummy4['expiry_time_diff_days']<=3671), 'expiry_days'] = 1
dummy4.loc[dummy4['expiry_time_diff_days']<3665, 'expiry_days'] = 0
final_dataset=dummy4.copy()
final_dataset=final_dataset.drop(['expiry_time_diff_days','booking_create_timestamp','booking_approved_at','booking_checkin_customer_date','booking_expiry_date'],axis=1)
payment_modes_reset={'debit_card':'other', 'not_defined':'other', 'credit_card':'credit_card','gift_card':'gift_card','voucher':'voucher'}
final_dataset['mode_payment_type2']=final_dataset['mode_payment_type'].map(payment_modes_reset)
final_dataset=final_dataset.drop(['mode_payment_type'],axis=1)
categorical_cols=['booking_status','booking_quarter','booking_year','booking_dayofweek','country','mode_payment_type2','hotel_category']
encoded_data = final_dataset
for col in categorical_cols:
    col_ohe = pd.get_dummies(final_dataset[col], prefix=col)
    encoded_data = pd.concat((encoded_data, col_ohe), axis=1).drop(col, axis=1)
X_train=pd.merge(train_data, encoded_data, on='booking_id', how="left")
Y_train=X_train['rating_score']
X_train=X_train.drop(['rating_score','booking_id'],axis=1)
X_test=pd.merge(test_data, encoded_data, on='booking_id', how="left")
X_test2=X_test.drop(['booking_id','rating_score'],axis=1)
from sklearn.preprocessing import StandardScaler
sc = StandardScaler()
X_train_std = sc.fit_transform(X_train)
X_test_std = sc.transform(X_test2)
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.ensemble import GradientBoostingClassifier
hgbr=HistGradientBoostingRegressor()
hgbr.fit(X_train_std,Y_train)
Y_test=hgbr.predict(X_test_std)
Y_test=pd.DataFrame(Y_test)
result=pd.concat([X_test,Y_test],axis=1)
result_final=result[['booking_id',0]]
max_type=result_final.groupby('booking_id',as_index=False).agg({0: lambda x:x.mode()[0]})
max_type=max_type.reset_index(drop=True).rename(columns={'0':'rating_score'})
result_final=pd.merge(result_final, max_type, on='booking_id', how="left")
result_final=result_final[['booking_id','0_y']]
result_final=result_final.rename(columns={'0_y':'rating_score'})
result_final=result_final.drop_duplicates().reset_index(drop=True)
result_final.to_csv(r'C:\Users\91836\Desktop\Curriculum and courses\Semesters\sem 7\PRML\Data Contest\output\BE19B002_BS19B028.csv',index=False)

