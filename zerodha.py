
url=''
#after login put your url in url

import time
import pyotp
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
from kiteconnect import exceptions
import numpy as np
import chromedriver_autoinstaller
from nsepy.derivatives import get_expiry_date
from urllib.parse import urlparse
from urllib.parse import parse_qs
from datetime import timedelta
import pandas as pd
import traceback
import requests
import datetime
import pdb
import os
import time
import mibian


class ZerodhaTrading:    

    def __init__(self,api_key:str,api_secret:str,instrument_flag:str):
        """Initializing Tradehull Object."""
        try:
            self.filename = str(datetime.datetime.now().date()) + ' token' + '.txt'
            self.params = {api_key:str,api_secret:str}
            self.check_if_parameter_is_correct(self.params)
            self.interval_parameters = {'minute':  15,'2minute':  15,'3minute':  15,'4minute':  15,'5minute':  15,'day':  999,'10minute':  15,'15minute':  15,'30minute':  30,'60minute':  200}
            self.index_step_dict = {"NIFTY 50":50,"NIFTY BANK":100,'NIFTY FIN SERVICE':50,'NIFTY MID SELECT':25,'SENSEX':100}
            self.index_underlying = {"NIFTY 50":"NIFTY","NIFTY BANK":"BANKNIFTY",'NIFTY FIN SERVICE':'FINNIFTY','NIFTY MID SELECT':'MIDCPNIFTY','SENSEX':'SENSEX'}
            self.kite = self.get_login(api_key,api_secret)
            # self.instrument_file = pd.DataFrame(self.kite.instruments())
            if instrument_flag == "yes":
                self.instrument_file = self.get_instrument_file()
                self.instrument_file['expiry'] = self.instrument_file['expiry'].astype(str)
            try:
                kite.pro
                self.step_df = pd.read_excel("https://www1.nseindia.com/content/fo/sos_scheme.xls")
            except Exception as e:
                print("step Value DF is not generated due to Error from NSE India site: ", e)
                print("Collecting step values from program memory.")
                step_value_dict = {'AARTIIND': 20, 'ACC': 20, 'ADANIENT': 20, 'ADANIPORTS': 10, 'ALKEM': 20, 'AMARAJABAT': 10, 'AMBUJACEM': 5, 'APLLTD': 10, 'APOLLOHOSP': 50, 'APOLLOTYRE': 5, 'ASHOKLEY': 2.5, 'ASIANPAINT': 20, 'AUBANK': 20, 'AUROPHARMA': 10, 'AXISBANK': 10, 'BAJAJ-AUTO': 50, 'BAJAJFINSV': 100, 'BAJFINANCE': 100, 'BALKRISIND': 20, 'BANDHANBNK': 5, 'BANKBARODA': 2.5, 'BATAINDIA': 20, 'BEL': 2.5, 'BERGEPAINT': 10, 'BHARATFORG': 10, 'BHARTIARTL': 10, 'BHEL': 1, 'BIOCON': 5, 'BOSCHLTD': 250, 'BPCL': 10, 'BRITANNIA': 20, 'CADILAHC': 5, 'CANBK': 5, 'CHOLAFIN': 10, 'CIPLA': 10, 'COALINDIA': 2.5, 'COFORGE': 50, 'COLPAL': 20, 'CONCOR': 10, 'CUB': 2.5, 'CUMMINSIND': 20, 'DABUR': 5, 'DEEPAKNTR': 20, 'DIVISLAB': 50, 'DLF': 5, 'DRREDDY': 50, 'EICHERMOT': 50, 'ESCORTS': 20, 'EXIDEIND': 2.5, 'FEDERALBNK': 1, 'GAIL': 2.5, 'GLENMARK': 5, 'GMRINFRA': 1, 'GODREJCP': 10, 'GODREJPROP': 20, 'GRANULES': 5, 'GRASIM': 20, 'GUJGASLTD': 10, 'HAVELLS': 20, 'HCLTECH': 10, 'HDFC': 50, 'HDFCAMC': 50, 'HDFCBANK': 20, 'HDFCLIFE': 10, 'HEROMOTOCO': 50, 'HINDALCO': 5, 'HINDPETRO': 5, 'HINDUNILVR': 20, 'IBULHSGFIN': 5, 'ICICIBANK': 10, 'ICICIGI': 20, 'ICICIPRULI': 5, 'IDEA': 1, 'IDFCFIRSTB': 1, 'IGL': 10,'INDIGO': 20, 'INDUSINDBK': 20, 'INDUSTOWER': 5, 'INFY': 20, 'IOC': 1, 'IRCTC': 20, 'ITC': 2.5, 'JINDALSTEL': 10, 'JSWSTEEL': 10, 'JUBLFOOD': 50, 'KOTAKBANK': 20, 'L&TFH': 2.5, 'LALPATHLAB': 50, 'LICHSGFIN': 10, 'LT': 20, 'LTI': 100, 'LTTS': 50, 'LUPIN': 20, 'M&M': 10, 'M&MFIN': 5, 'MANAPPURAM': 2.5, 'MARICO': 5, 'MARUTI': 100, 'MCDOWELL-N': 5, 'MFSL': 20, 'MGL': 20, 'MINDTREE': 20, 'MOTHERSUMI': 5, 'MPHASIS': 20, 'MRF': 500, 'MUTHOOTFIN': 20, 'NAM-INDIA': 5, 'NATIONALUM': 1, 'NAUKRI': 100, 'NAVINFLUOR': 50, 'NESTLEIND': 100, 'NMDC': 2.5, 'NTPC': 1, 'ONGC': 2.5, 'PAGEIND': 500, 'PEL': 50, 'PETRONET': 5, 'PFC': 2.5, 'PFIZER': 50, 'PIDILITIND': 20, 'PIIND': 20, 'PNB': 1, 'POWERGRID': 2.5, 'PVR': 20, 'RAMCOCEM': 10, 'RBLBANK': 5, 'RECLTD': 2.5, 'RELIANCE': 20, 'SAIL': 5, 'SBILIFE': 10, 'SBIN': 5, 'SHREECEM': 500, 'SIEMENS': 20, 'SRF': 100, 'SRTRANSFIN': 20, 'SUNPHARMA': 10, 'SUNTV': 10, 'TATACHEM': 10, 'TATACONSUM': 10, 'TATAMOTORS': 5, 'TATAPOWER': 2.5, 'TATASTEEL': 20, 'TCS': 50, 'TECHM': 20, 'TITAN': 20, 'TORNTPHARM': 20, 'TORNTPOWER': 5, 'TRENT': 20, 'TVSMOTOR': 10, 'UBL': 20, 'ULTRACEMCO': 100, 'UPL': 10, 'VEDL': 5, 'VOLTAS': 20, 'WIPRO': 5, 'ZEEL': 5}
                self.step_df = pd.DataFrame.from_dict(step_value_dict, orient='index')
                self.step_df = self.step_df.reset_index()
                self.step_df.rename({"index": "Symbol", 0: "Applicable Step value"}, axis='columns', inplace =True)
            print(f"you are connected to zerodha {self.kite.profile()['user_name']} " )
        except Exception as e:
            print(e)
            traceback.print_exc()

    def get_instrument_file(self):
        current_date = time.strftime("%Y-%m-%d")
        expected_file = 'all_instrument ' + str(current_date) + '.csv'
        for item in os.listdir():
            path = os.path.join(item)
            
            if (item.startswith('all_instrument')) and (current_date not in  item.split(" ")[1]):
                if os.path.isfile(path):
                    os.remove(path)

        if expected_file in os.listdir():
            try:
                print(f"reading existing file {expected_file}")
                instrument = pd.read_csv(expected_file)
                if 'Unnamed: 0' in instrument.columns:
                    instrument = instrument.drop(['Unnamed: 0'], axis = 1)
                
            except Exception as e:
                print("Instrument file is not generated completely, picking new file from zerodha Again ")
                instrument = self.kite.instruments()
                instrument = pd.DataFrame(instrument)
                instrument.to_csv(expected_file)
        else:
            # this will fetch instrument file from zerodha
            print("picking new file from zerodha")
            instrument = self.kite.instruments()
            instrument = pd.DataFrame(instrument)
            instrument.to_csv(expected_file)
        return instrument


    def read_access_token_from_file(self):
        file = open(self.filename, 'r+')
        access_token = file.read()
        file.close()
        return access_token


    def send_access_token_to_file(self,access_token):   
        file = open(self.filename, 'w')
        file.write(access_token)
        file.close()


    def get_login(self, api_k, api_s):
        self.kite = KiteConnect(api_key=api_k)
        print("Logging into zerodha")


        if self.filename not in os.listdir():
            print("[*] Generate Request Token : ", self.kite.login_url())
            token_url = url
            request_tkn = parse_qs(urlparse(token_url).query)['request_token'][0]
            data = self.kite.generate_session(request_tkn, api_secret=api_s)
            access_token = data["access_token"]
            self.kite.set_access_token(access_token)
            self.kws = KiteTicker(api_k, access_token)
            self.send_access_token_to_file(access_token)

        elif self.filename in os.listdir():
            print("You have already loggged in for today")
            
            access_token = self.read_access_token_from_file()

            self.kite.set_access_token(access_token)
            self.kws = KiteTicker(api_k, access_token)

        return self.kite


    def get_last_tradable_day(self):
        today = datetime.datetime.now().date()
        prev_date = today - datetime.timedelta(days=20)
        df = self.kite.historical_data(instrument_token=5633, from_date=prev_date, to_date=today, interval='day', continuous=False, oi=False)
        df = pd.DataFrame(df)
        if (str(today) == str(df.iloc[-1]['date'])[:10]):
            last_tradable_date = str(df.iloc[-2]['date'])[:10]
        if (str(today) != str(df.iloc[-1]['date'])[:10]):
            last_tradable_date = str(df.iloc[-1]['date'])[:10]
        return last_tradable_date


    def check_if_parameter_is_correct(self,params:dict):
        """To check if given parameter is correct for specific funtion\n
        This funtion is confirm wheather we are getting correct input from accessor"""
        flag = True
        for i in params:
            if params[i] == type(i):
                flag = True
            else:
                flag = False
            
            if flag == False:
                error_msg = f"{i} is a type of {type(i)}, required type is {params[i]}"
                raise TypeError(error_msg)
        

    def get_short_length_hist_data(self,name:str,exchange:str,interval:str,oi:bool=False) -> pd.core.frame.DataFrame:
        """This function will give you short term historical data\n
        you will get data as per kite api limitation as below \n
        "minute": 30 days,\n
        "hour": 365 days,\n
        "day": 2000 days,\n
        "3minute": 90 days,\n
        "5minute": 90 days,\n
        "10minute": 90 days,\n
        "15minute": 180 days,\n
        "30minute": 180 days,\n
        "60minute": 365 days"""
        try:
            self.params = {name:str,exchange:str,interval:str,oi:bool}
            res = self.check_if_parameter_is_correct(self.params)
            days_buffer = self.interval_parameters[interval]
            to_date = datetime.datetime.today().date()
            from_date = to_date - datetime.timedelta(days_buffer)
            instrument_token = self.kite.ltp(exchange+":"+name)[exchange+":"+name]['instrument_token']
            data = self.kite.historical_data(instrument_token=instrument_token, from_date=from_date,to_date=to_date, interval=interval, continuous=False, oi=oi)
            data = pd.DataFrame(data)
            return data
        except (Exception,exceptions.DataException, exceptions.NetworkException) as e:
            print(e)
            traceback.print_exc()
            pass


    def get_short_length_hist_data_specific_dur(self,name:str,exchange:str,interval:str,from_date:str,to_date:str,oi:bool=False) -> pd.core.frame.DataFrame:
        """This function will give you short term historical data\n
        you will get data as per kite api limitation as below \n
        "minute": 30 days,\n
        "hour": 365 days,\n
        "day": 2000 days,\n
        "3minute": 90 days,\n
        "5minute": 90 days,\n
        "10minute": 90 days,\n
        "15minute": 180 days,\n
        "30minute": 180 days,\n
        "60minute": 365 days"""
        try:
            self.params = {name:str,exchange:str,interval:str,oi:bool}
            res = self.check_if_parameter_is_correct(self.params)
            days_buffer = self.interval_parameters[interval]
            to_date = to_date
            from_date = from_date
            instrument_token = self.kite.ltp(exchange+":"+name)[exchange+":"+name]['instrument_token']
            data = self.kite.historical_data(instrument_token=instrument_token, from_date=from_date,to_date=to_date, interval=interval, continuous=False, oi=oi)
            data = pd.DataFrame(data)
            return data
        except (Exception,exceptions.DataException, exceptions.NetworkException) as e:
            print(e)
            traceback.print_exc()
            pass


    def get_long_length_hist_data(self,name:str,exchange:str,interval:str,length:int,oi:bool=False) -> pd.core.frame.DataFrame:
        """
            To avoid kite api data limitation we can download data from the date when stock is been listed.
            this function will work for all the stock except option.
            Note: If you want to download longterm historical data for option, you can approch to GDFL(this will be paid)
        """
        try:
            self.params = {name:str,exchange:str,interval:str,length:int,oi:bool}
            res = self.check_if_parameter_is_correct(self.params)
            to_date = datetime.datetime.today().date()
            from_date = (to_date - datetime.timedelta(length))
            delta_value_dict = {'minute': 60, '3minute': 100, '5minute': 100, '10minute': 100, '15minute': 200, '30minute': 200, '60minute': 400, 'day': 2000}
            instrument_token = self.kite.ltp(exchange+":"+name)[exchange+":"+name]['instrument_token']
            culumative_data = []
            delta_value = delta_value_dict[interval]
            while True:
                to_date = from_date + datetime.timedelta(days=delta_value)
                data = self.kite.historical_data(instrument_token=instrument_token, from_date=from_date, to_date=to_date, interval=interval, continuous=False, oi=False)
                for candle in data:
                    culumative_data.append(candle)

                print(name, from_date, to_date, len(culumative_data))
                from_date = to_date + datetime.timedelta(days=1)
                if to_date > datetime.datetime.now().date():
                    break
            data = pd.DataFrame(culumative_data)
            return data
        except (Exception,exceptions.DataException, exceptions.NetworkException) as e:
            print(e)
            traceback.print_exc()
            pass


    def get_long_length_hist_data_specific_dur(self,name:str,exchange:str,interval:str,from_date:str,download_till_date:str,oi:bool=False) -> pd.core.frame.DataFrame:
        """
            To avoid kite api data limitation we can download data from the date when stock is been listed.
            this function will work for all the stock except option.
            Note: If you want to download longterm historical data for option, you can approch to GDFL(this will be paid)
        """
        try:
            self.params = {name:str,exchange:str,interval:str,oi:bool,from_date:str,download_till_date:str }
            res = self.check_if_parameter_is_correct(self.params)
            delta_value_dict = {'minute': 60, '3minute': 100, '5minute': 100, '10minute': 100, '15minute': 200, '30minute': 200, '60minute': 400, 'day': 2000}
            delta_value = delta_value_dict[interval]
            # to_date = from_date + datetime.timedelta(days=delta_value)
            download_till_date = download_till_date
            to_date = None
            from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
            from_date = from_date.date()
            download_till_date = datetime.datetime.strptime(download_till_date, "%Y-%m-%d")
            download_till_date = download_till_date.date()
            instrument_token = self.kite.ltp(exchange+":"+name)[exchange+":"+name]['instrument_token']
            culumative_data = []
            delta_value = delta_value_dict[interval]
            while True:
                to_date = from_date + datetime.timedelta(days=delta_value)
                if from_date < download_till_date < to_date:
                    to_date = download_till_date
                data = self.kite.historical_data(instrument_token=instrument_token, from_date=from_date, to_date=to_date, interval=interval, continuous=False, oi=False)
                for candle in data:
                    culumative_data.append(candle)

                print(name, from_date, to_date, len(culumative_data))
                from_date = to_date + datetime.timedelta(days=1)
                if to_date >= download_till_date:
                    break
            data = pd.DataFrame(culumative_data)
            return data
        except (Exception,exceptions.DataException, exceptions.NetworkException) as e:
            print(e)
            traceback.print_exc()
            pass

    def get_intraday_allowed_script(self) -> list:
        """
            This function will return the MIS allowed stock for zerodha
        """
        try:
            sheet_url = "https://docs.google.com/spreadsheets/d/1ZTyh6GiHTwA1d-ApYdn5iCmRiBLZoAtwigS7VyLUk_Y/edit#gid=0"
            url_1 = sheet_url.replace('/edit#gid=', '/export?format=csv&gid=')
            allowed_script = pd.read_csv(url_1)
            allowed_script = allowed_script['Stocks allowed for MIS'].to_list()
            return allowed_script
        except Exception as e:
            print(e)
            traceback.print_exc()


    def get_data_for_single_script(self,exchange:str,name:str,call_type:str) -> dict:
        self.params = {exchange:str,name:str,call_type:str}
        res = self.check_if_parameter_is_correct(self.params)

        try:
            if call_type == 'ohlc':
                return self.kite.ohlc(exchange+str(":")+name)[exchange+str(":")+name]['ohlc']

            if call_type == 'ltp':
                return self.kite.ltp(exchange+str(":")+name)[exchange+str(":")+name]['last_price']

            if call_type == 'quote':
                return self.kite.quote(exchange+str(":")+name)[exchange+str(":")+name]

        except Exception as e:
            print(e)
            traceback.print_exc()


    # def get_atm(self,ltp,underlying,expiry,step,script_type) -> str:
    def get_atm(self,ltp,underlying,expiry,script_type) -> str:
        """
        ltp should be float or int
        ltp step be float or int
        underlying and expiry should be string
        expiry should be in format integer
        0 - curent expiry
        1 - next expiry
        2 - third expiry from now 
        """
        try:
            if underlying in self.step_df['Symbol'].to_list():
                step = self.step_df[self.step_df['Symbol'] == underlying]['Applicable Step value'].iloc[0]

            elif underlying in self.index_step_dict.keys():
                step = self.index_step_dict[underlying]
                underlying = self.index_underlying[underlying]
            
            else:
                raise TypeError('Unknown underlying name')

            strike = round(ltp/step)*step
            data = self.instrument_file[(self.instrument_file['name'] == underlying) & (self.instrument_file['strike'] == float(strike)) & (self.instrument_file['instrument_type'] == script_type)]
            data = data.sort_values('expiry')
            
            if len(data)>0:
                if expiry >= len(data):
                    expiry = -1
                else:
                    pass
                return data.iloc[expiry]['tradingsymbol']
            else:
                raise TypeError("check input parameter correctly for get_atm()")

        except Exception as e:
            print(e)
            traceback.print_exc()


    def get_itm(self,ltp,underlying,expiry,multiplier,script_type):
        """
        ltp should be float or int
        ltp step be float or int
        underlying should be string
        multiplier - how much step you want to set
        expiry should be in format integer
        0 - curent expiry
        1 - next expiry
        2 - third expiry from now 
        """
        
        try:
            if underlying in self.step_df['Symbol'].to_list():
                step = self.step_df[self.step_df['Symbol'] == underlying]['Applicable Step value'].iloc[0]

            elif underlying in self.index_step_dict.keys():
                step = self.index_step_dict[underlying]
                underlying = self.index_underlying[underlying]
            
            else:
                raise TypeError('Unknown underlying name')

            
            if script_type == 'CE':
                atm_strike = round(ltp/step)*step
                strike = atm_strike - (step*multiplier)
            
            elif script_type == 'PE':
                atm_strike = round(ltp/step)*step
                strike = atm_strike + (step*multiplier)
            else:
                raise TypeError("check input parameter correctly for get_itm()")
            
            data = self.instrument_file[(self.instrument_file['name'] == underlying) & (self.instrument_file['strike'] == float(strike)) & (self.instrument_file['instrument_type'] == script_type)]
            data = data.sort_values('expiry')
            
            if len(data)>0:
                if expiry >= len(data):
                    expiry = -1
                else:
                    pass
                return data.iloc[expiry]['tradingsymbol']
            else:
                raise TypeError("check input parameter correctly for get_atm()")
            

        except Exception as e:
            print(e)
            traceback.print_exc()


    def get_otm(self,ltp,underlying,expiry,multiplier,script_type):
        """
            ltp should be float or int
            ltp step be float or int
            underlying should be string
            multiplier - how much step you want to set
            expiry should be in format integer
            0 - curent expiry
            1 - next expiry
            2 - third expiry from now 
        """
        try:
            if underlying in self.step_df['Symbol'].to_list():
                step = self.step_df[self.step_df['Symbol'] == underlying]['Applicable Step value'].iloc[0]

            elif underlying in self.index_step_dict.keys():
                step = self.index_step_dict[underlying]
                underlying = self.index_underlying[underlying]
            
            else:
                raise TypeError('Unknown underlying name')

            
            if script_type == 'CE':
                atm_strike = round(ltp/step)*step
                strike = atm_strike + (step*multiplier)
            
            elif script_type == 'PE':
                atm_strike = round(ltp/step)*step
                strike = atm_strike - (step*multiplier)
            else:
                raise TypeError("check input parameter correctly for get_otm()")
            
            data = self.instrument_file[(self.instrument_file['name'] == underlying) & (self.instrument_file['strike'] == float(strike)) & (self.instrument_file['instrument_type'] == script_type)]
            data = data.sort_values('expiry')
            
            if len(data)>0:
                if expiry >= len(data):
                    expiry = -1
                else:
                    pass
                return data.iloc[expiry]['tradingsymbol']
            else:
                raise TypeError("check input parameter correctly for get_atm()")
            

        except Exception as e:
            print(e)
            traceback.print_exc()


    

    def check_valid_instrument(self,name):
        try:
            df = self.instrument_file[self.instrument_file['tradingsymbol']==name]
            if len(df) != 0:
                return f"instrument {name} is valid"
            else:
                return f"instrument {name} is invalid"

        except Exception as e:
            print(e)
            traceback.print_exc()

    def get_tradable_days(self,no_of_days:int):
        """
            This function returns the number of last trading days
        """
        self.params = {no_of_days:int}
        res = self.check_if_parameter_is_correct(self.params)
        try:
            instrument_token = self.kite.ltp("NSE:ACC")['NSE:ACC']['instrument_token']
            from_date = (datetime.datetime.now()-timedelta(days=700)).date()
            to_date = datetime.datetime.now().date()
            data = self.kite.historical_data(instrument_token=instrument_token, from_date=from_date, to_date=to_date, interval='day', continuous=False, oi=False)
            data = pd.DataFrame(data)
            dates = [str(date) for date in data['date'].to_list()]
            return dates[-no_of_days:]
        except Exception as e:
            print(e)
            traceback.print_exc()


    def monthly_resample_data(self,df):
        '''
            this function will return monthly resample data.
            resample is the process to combine 
        '''
        try:
            logic = {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}
            df = df.set_index(df['date'])
            resample_data = df.resample('M').agg(logic)
            return resample_data
        except Exception as e:
            print(e)
            traceback.print_exc()


    def hourly_resample_data(self,df,time_frame:int):
        self.params = {time_frame:int}
        res = self.check_if_parameter_is_correct(self.params)

        try:
            logic = {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}
            timeframe = str(time_frame)+"H"
            df = df.set_index('date')
            df = df.resample(timeframe, origin= "start").agg(logic)
            df = df[df.isna().any(axis=1) == False]
            return df

        except Exception as e:
            print(e)
            traceback.print_exc()

    def weekly_resample_data(self,df):
        try:
            """
                this function may have some some ambuiguity in data
            """

            logic = {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}
            df = df.set_index('date')
            df = df.resample('W-FRI', origin= "start").agg(logic)
            df = df[df.isna().any(axis=1) == False]
            df
            return df

        except Exception as e:
            print(e)
            traceback.print_exc()


    def get_completed_candle_data(self, df):
        if datetime.datetime.now().time() < datetime.time(15,30):
            completed_df = df.reset_index()
            completed_candle_data = df.iloc[-2]
        if datetime.datetime.now().time() > datetime.time(15,30):
            completed_df = df.reset_index()
            completed_candle_data = df.iloc[-1]
        return completed_candle_data


    def minute_resample_data(self,df,timeframe):
        try:
            """
                this function may have some some ambuiguity in data
            """
            timeframe = str(timeframe)+"Min"
            logic = {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}
            df = df.set_index('date')
            df = df.resample(timeframe, origin="start").agg(logic)
            # df = df.resample('W-FRI', origin= "start").agg(logic)
            df = df[df.isna().any(axis=1) == False]
            return df

        except Exception as e:
            print(e)
            traceback.print_exc()



    def market_over_close_all_order(self):
        try:
            # close pending trades
            orders = pd.DataFrame(self.kite.orders())
            if orders.empty:
                return

            trigger_pending_orders = orders.loc[(orders['status'] == 'TRIGGER PENDING') & (orders['product'] == 'NRML')]
            open_orders = orders.loc[(orders['status'] == 'OPEN') & (orders['product'] == 'NRML')]
            for index, row in trigger_pending_orders.iterrows():
                self.kite.cancel_order(variety=row['variety'], order_id=row['order_id'])

            for index, row in open_orders.iterrows():
                self.kite.cancel_order(variety=row['variety'], order_id=row['order_id'])


            position = pd.DataFrame(self.kite.positions()['net'])

            if position.empty :
                return

            positive_trades = position.loc[(position['quantity'] > 0) & (position['product'] == 'NRML')]

            negative_trades = position.loc[(position['quantity'] < 0) & (position['product'] == 'NRML')]

            for index, row in negative_trades.iterrows():
                quantity = row['quantity'] * -1
                ltp_negative=self.kite.ltp('NFO'+str(":")+row['tradingsymbol'])['NFO'+str(":")+row['tradingsymbol']]['last_price']
                self.kite.place_order(variety=self.kite.VARIETY_REGULAR, exchange=row['exchange'], tradingsymbol=row['tradingsymbol'],transaction_type=self.kite.TRANSACTION_TYPE_BUY, quantity=quantity, product=self.kite.PRODUCT_NRML, order_type=self.kite.ORDER_TYPE_LIMIT,price=round(ltp_negative/100*110,1))
                time.sleep(1)
            for index, row in positive_trades.iterrows():
                ltp_positive=self.kite.ltp('NFO'+str(":")+row['tradingsymbol'])['NFO'+str(":")+row['tradingsymbol']]['last_price']
                self.kite.place_order(variety=self.kite.VARIETY_REGULAR, exchange=row['exchange'], tradingsymbol=row['tradingsymbol'],transaction_type=self.kite.TRANSACTION_TYPE_SELL, quantity=row['quantity'], product=self.kite.PRODUCT_NRML, order_type=self.kite.ORDER_TYPE_LIMIT,price=round(ltp_positive/100*90,1))
                time.sleep(1)
            
        except Exception as e:
            print(e)
            traceback.print_exc()

    def market_over_close_all_order_expiry(self):
        try:
            # close pending trades
            orders = pd.DataFrame(self.kite.orders())
            if orders.empty:
                return

            trigger_pending_orders = orders.loc[(orders['status'] == 'TRIGGER PENDING') & (orders['product'] == 'NRML')]
            open_orders = orders.loc[(orders['status'] == 'OPEN') & (orders['product'] == 'NRML')]
            for index, row in trigger_pending_orders.iterrows():
                self.kite.cancel_order(variety=row['variety'], order_id=row['order_id'])

            for index, row in open_orders.iterrows():
                self.kite.cancel_order(variety=row['variety'], order_id=row['order_id'])


            position = pd.DataFrame(self.kite.positions()['net'])

            if position.empty :
                return

            positive_trades = position.loc[(position['quantity'] > 0) & (position['product'] == 'NRML')]

            negative_trades = position.loc[(position['quantity'] < 0) & (position['product'] == 'NRML')]

            for index, row in negative_trades.iterrows():
                quantity = row['quantity'] * -1
                ex='NFO'
                if row['tradingsymbol'][0:5]=='SENSE':
                    ex='BFO'
                ltp_negative=self.kite.ltp(ex+str(":")+row['tradingsymbol'])[ex+str(":")+row['tradingsymbol']]['last_price']
                if ltp_negative<5:
                    ltp_negative=5
                self.kite.place_order(variety=self.kite.VARIETY_REGULAR, exchange=row['exchange'], tradingsymbol=row['tradingsymbol'],transaction_type=self.kite.TRANSACTION_TYPE_BUY, quantity=quantity, product=self.kite.PRODUCT_NRML, order_type=self.kite.ORDER_TYPE_LIMIT,price=round(ltp_negative/100*110,1))
                time.sleep(1)

            for index, row in positive_trades.iterrows():
                ex='NFO'
                if row['tradingsymbol'][0:5]=='SENSE':
                    ex='BFO'
                ltp_positive=self.kite.ltp(ex+str(":")+row['tradingsymbol'])[ex+str(":")+row['tradingsymbol']]['last_price']
                if ltp_positive<5:
                    ltp_positive=0.10
                self.kite.place_order(variety=self.kite.VARIETY_REGULAR, exchange=row['exchange'], tradingsymbol=row['tradingsymbol'],transaction_type=self.kite.TRANSACTION_TYPE_SELL, quantity=row['quantity'], product=self.kite.PRODUCT_NRML, order_type=self.kite.ORDER_TYPE_LIMIT,price=round(ltp_positive/100*90,1))
                time.sleep(1)

        except Exception as e:
            print(e)
            traceback.print_exc()
    
    def market_over_close_all_order_expiry_partial(self,option_four,exc_name):
        try:
            # close pending trades
            orders = pd.DataFrame(self.kite.orders())
            if orders.empty:
                return

            trigger_pending_orders = orders.loc[(orders['status'] == 'TRIGGER PENDING') & (orders['product'] == 'NRML')]
            open_orders = orders.loc[(orders['status'] == 'OPEN') & (orders['product'] == 'NRML')]
            for index, row in trigger_pending_orders.iterrows():
                self.kite.cancel_order(variety=row['variety'], order_id=row['order_id'])

            for index, row in open_orders.iterrows():
                self.kite.cancel_order(variety=row['variety'], order_id=row['order_id'])


            position = pd.DataFrame(self.kite.positions()['net'])

            if position.empty :
                return

            positive_trades = position.loc[(position['quantity'] > 0) & (position['product'] == 'NRML')]

            negative_trades = position.loc[(position['quantity'] < 0) & (position['product'] == 'NRML')]

            for index, row in negative_trades.iterrows():
                if row['tradingsymbol'][0:6]!=option_four:
                    continue
                quantity = row['quantity'] * -1
                ltp_negative=self.kite.ltp(exc_name+str(":")+row['tradingsymbol'])[exc_name+str(":")+row['tradingsymbol']]['last_price']
                if ltp_negative<5:
                    ltp_negative=5
                self.kite.place_order(variety=self.kite.VARIETY_REGULAR, exchange=row['exchange'], tradingsymbol=row['tradingsymbol'],transaction_type=self.kite.TRANSACTION_TYPE_BUY, quantity=quantity, product=self.kite.PRODUCT_NRML, order_type=self.kite.ORDER_TYPE_LIMIT,price=round(ltp_negative/100*115,1))
                time.sleep(1)

            for index, row in positive_trades.iterrows():
                if row['tradingsymbol'][0:6]!=option_four:
                    continue
                ltp_positive=self.kite.ltp(exc_name+str(":")+row['tradingsymbol'])[exc_name+str(":")+row['tradingsymbol']]['last_price']
                if ltp_positive<5:
                    ltp_positive=0.10
                self.kite.place_order(variety=self.kite.VARIETY_REGULAR, exchange=row['exchange'], tradingsymbol=row['tradingsymbol'],transaction_type=self.kite.TRANSACTION_TYPE_SELL, quantity=row['quantity'], product=self.kite.PRODUCT_NRML, order_type=self.kite.ORDER_TYPE_LIMIT,price=round(ltp_positive/100*85,1))
                time.sleep(1)

        except Exception as e:
            print(e)
            traceback.print_exc()


    def get_fut_scripts(self,underlying:str):
        self.params = {underlying:str}
        res = self.check_if_parameter_is_correct(self.params)
        try:
            data = self.instrument_file[(self.instrument_file['name'] == underlying) & (self.instrument_file['segment'] == 'NFO-FUT')].sort_values(by='expiry')
            
            if len(data)==0:
                data = self.instrument_file[(self.instrument_file['name'] == underlying) & (self.instrument_file['segment'] == 'MCX-FUT')].sort_values(by='expiry')
                
                if len(data)==0:
                    data = self.instrument_file[(self.instrument_file['name'] == underlying) & (self.instrument_file['segment'] == 'CDS-FUT')].sort_values(by='expiry')

                    if len(data)==0:
                        data = self.instrument_file[(self.instrument_file['name'] == underlying) & (self.instrument_file['segment'] == 'BCD-FUT')].sort_values(by='expiry')

        except Exception as e:
            print(e)
            print("problem in given underlying")
            traceback.print_exc()
           
        return data['tradingsymbol'].to_list()



    def get_expiries(self,scripname:str,exchange:str,expiry_type: str):
        self.params = {scripname:str,exchange:str,expiry_type: str}
        res = self.check_if_parameter_is_correct(self.params)
        if exchange == 'NSE' or exchange == 'BSE':
            raise NameError("This exchange doesn't contain Expiry")
            return []

        data = self.instrument_file[(self.instrument_file['exchange'] == exchange) & (self.instrument_file['name'] == scripname)].sort_values(by='expiry')
        if len(data) == 0:
            return []

        else:
            final_list = []
            expiries = [i for i in set(data['expiry'].to_list())]
            expiries = sorted(expiries)
            expiries_dict = {date[:7]:[] for date in expiries}
            for date in expiries:
                for exp in expiries_dict.keys():
                    if date[:7] == exp:
                        expiries_dict[exp].append(date)
            if expiry_type=="monthly":
                for expiry in expiries_dict:
                    final_list.append(expiries_dict[expiry][-1])
                return final_list[:3]
            if expiry_type=="weekly":
                today = datetime.datetime.now().date()
                year = today.year
                month = today.month
                key = str(year)+"-"+str(month)
                this_month_exiries = expiries_dict[key]
                if len(this_month_exiries) == 1:
                    return []
                else: 
                    return this_month_exiries[:-1]
            if expiry_type=="all":
                return expiries



    def get_option_greek(self, strike:int, expiry_date:str, asset:str, interest_rate:float, flag:str, scrip_type:str):
        # Get option Greeks calculations on https://github.com/yassinemaaroufi/MibianLib
        self.params = {strike:int, expiry_date:str, asset:str, interest_rate:int, flag:str, scrip_type:str}
        res = self.check_if_parameter_is_correct(self.params)
        
        if asset == 'NIFTY BANK':
            inst_asset = "BANKNIFTY"
        if asset == 'NIFTY 50':
            inst_asset = "NIFTY"
        
        data = self.instrument_file[(self.instrument_file['expiry'] == expiry_date) & (self.instrument_file['name'] == inst_asset) & (self.instrument_file['strike'] == float(strike))]
        script_list = data['tradingsymbol'].to_list()
        for name in script_list:
            if scrip_type in name:
                script = name
        days_delta = datetime.datetime.strptime(expiry_date, "%Y-%m-%d").date() - datetime.datetime.now().date()
        days_to_expiry = days_delta.days
        if days_to_expiry == 0:
            days_to_expiry = 1
        asset_price = self.get_data_for_single_script("NSE",asset,"ltp")
        ltp = self.get_data_for_single_script("NFO",script,"ltp")
        if scrip_type == 'CE':
            civ = mibian.BS([asset_price, strike, interest_rate, days_to_expiry], callPrice= ltp)
            cval = mibian.BS([asset_price, strike, interest_rate, days_to_expiry], volatility = civ.impliedVolatility ,callPrice= ltp)
            if flag == "price":
                return cval.callPrice
            if flag == "delta":
                return cval.callDelta
            if flag == "delta2":
                return cval.callDelta2
            if flag == "theta":
                return cval.callTheta
            if flag == "rho":
                return cval.callRho
            if flag == "vega":
                return cval.vega
            if flag == "gamma":
                return cval.gamma
            if flag == "all_val":
                return {'callPrice' : cval.callPrice, 'callDelta' : cval.callDelta, 'callDelta2' : cval.callDelta2, 'callTheta' : cval.callTheta, 'callRho' : cval.callRho, 'vega' : cval.vega, 'gamma' : cval.gamma}

        if scrip_type == "PE":
            piv = mibian.BS([asset_price, strike, interest_rate, days_to_expiry], putPrice= ltp)
            pval = mibian.BS([asset_price, strike, interest_rate, days_to_expiry], volatility = piv.impliedVolatility ,putPrice= ltp)
            if flag == "price":
                return pval.putPrice
            if flag == "delta":
                return pval.putDelta
            if flag == "delta2":
                return pval.putDelta2
            if flag == "theta":
                return pval.putTheta
            if flag == "rho":
                return pval.putRho
            if flag == "vega":
                return pval.vega
            if flag == "gamma":
                return pval.gamma
            if flag == "all_val":
                return {'callPrice' : pval.putPrice, 'callDelta' : pval.putDelta, 'callDelta2' : pval.putDelta2, 'callTheta' : pval.putTheta, 'callRho' : pval.putRho, 'vega' : pval.vega, 'gamma' : pval.gamma}



    def get_lot_size(self,script_name:str):
        self.params = {script_name:str}
        res = self.check_if_parameter_is_correct(self.params)
        data = self.instrument_file[(self.instrument_file['tradingsymbol'] == script_name)]
        if len(data) == 0:
            raise NameError("Enter valid Scrip Name")
            return None

        else:
            return data.iloc[0]['lot_size'] 

    def get_order_status(self,order_id):
        flag = True
        while flag:
            status = self.kite.order_history(order_id)[-1]['status']
            flag = False
        return status

    def get_executed_price(self,order_id):
        flag = True
        while flag:
            price = self.kite.order_history(order_id)[-1]['average_price']
            flag = False
        return price


    def get_executed_time(self,order_id):
        flag = True
        while flag:
            order_time = str(self.kite.order_history(order_id)[-1]['order_timestamp'])
            flag = False
        return order_time


    def get_index_data(self):
        """
            This function will return live data for all indices worked in indian market
        """
        try:
            filter_df = self.instrument_file[self.instrument_file['segment'] == 'INDICES']
            watchlist = [row['exchange']+":"+row['name'] for index,row in filter_df.iterrows()]
            data = {}
            tick_price = self.kite.ltp(watchlist)
            num = 0
            for i in tick_price:
                name = i.split(":")[1]
                data[num] = {'name':name,'last_price':tick_price[i]['last_price']}
                num+=1
            data = pd.DataFrame(data).T
            return data
        except Exception as e:
            print(e)
            traceback.print_exc()


    def get_straddle(self,underlying:str,step:float,ltp:float,expiry:str):
        """
            This function is intended to return script for straddle
        """
        try:
            strike = round(ltp/step)*step
            data = self.instrument_file[(self.instrument_file['expiry'] == expiry) & (self.instrument_file['name'] == underlying) & (self.instrument_file['strike'] == float(strike))]

            if len(data)!=0:
                straddle = data['tradingsymbol'].to_list()
                return straddle
            else:
                raise TypeError("check input parameter correctly for get_straddle()")
        except Exception as e:
            TypeError("check input parameter correctly for get_straddle()")



    def get_strangle(self,underlying:str,step:float,ltp:float,expiry:str,multiplier:int):
        """

            this funtion returns call/put scripts from both side previous and next strike to make strangle as per requirements
        """

        try:
            if multiplier == 0:
                multiplier = 1

            strike = round(ltp/step)*step

            prevstrike = strike - (step*multiplier)
            aheadstrike = strike + (step*multiplier)

            global put,call
            put = str(prevstrike)+"PE"         
            call = str(aheadstrike)+"CE"

            data = self.instrument_file[(self.instrument_file['expiry'] == expiry) & (self.instrument_file['name'] == underlying) & ((self.instrument_file['strike'] == float(prevstrike)) | (self.instrument_file['strike'] == float(aheadstrike)))]

            if len(data)!=0:
                strangle_list = data['tradingsymbol'].to_list()
                strangle = [name for name in strangle_list if (put in name) or (call in name)]
                return strangle
            else:
                raise TypeError("check input parameter correctly for get_strangle()")
        except Exception as e:
            TypeError("check input parameter correctly for get_strangle()")


    def get_live_pnl(self):
        """
            to overcome the issue which we are facing in kite positionbook api end point, it has not showing love pnl.
            so as per the suggestion given on forum we have made custom calculation
        """

        try:
            df_pnl = self.kite.positions()['net']

            if len(df_pnl)==0:
                return 0
            script_list = []

            for pos in df_pnl:

                names = pos['exchange']+':'+pos['tradingsymbol']
                script_list.append(names)

            last_price_forpnl = self.kite.ltp(script_list)

            for pos in df_pnl:

                names = pos['exchange']+':'+pos['tradingsymbol']
                df_pnl[df_pnl.index(pos)]['pnl'] = (pos['sell_value'] - pos['buy_value']) + (pos['quantity'] * last_price_forpnl[names]['last_price'] * pos['multiplier'])
            
            df_pnl = pd.DataFrame(df_pnl)
            return df_pnl['pnl'].sum()

        except Exception as e:
            print(e)
    
    def get_live_pnl_partial(self,option_four):
        """
            to overcome the issue which we are facing in kite positionbook api end point, it has not showing love pnl.
            so as per the suggestion given on forum we have made custom calculation
        """
        try:
            df_pnl = pd.DataFrame(self.kite.positions()['net'])
            df_pnl['scrip_four'] = np.nan
            for index, row in df_pnl.iterrows():
                df_pnl.loc[index,'scrip_four'] = row['tradingsymbol'][0:6]

                #df_pnl['script_fo']=row['tradingsymbol'][0:4]
            df_pnl=df_pnl.loc[(df_pnl['scrip_four']==option_four)]

            if len(df_pnl)==0:
                return 0
            
            script_list = []
            for index,pos in df_pnl.iterrows():

                names = pos['exchange']+':'+pos['tradingsymbol']
                script_list.append(names)

            last_price_forpnl = self.kite.ltp(script_list)

            # Assuming df_pnl is your DataFrame and last_price_forpnl is defined
            for index, pos in df_pnl.iterrows():
                names = pos['exchange'] + ':' + pos['tradingsymbol']
                # Calculate P&L
                pnl = (pos['sell_value'] - pos['buy_value']) + (pos['quantity'] * last_price_forpnl[names]['last_price'] * pos['multiplier'])
                # Update the 'pnl' column in the DataFrame
                df_pnl.at[index, 'pnl'] = pnl

            df_pnl = pd.DataFrame(df_pnl)
            return df_pnl['pnl'].sum()

        except Exception as e:
            print(e)

    
    def check_sl_tgt_order(self,sl_orderid,tgt_orderid):
        """
            This function will help you to cancel SL order if target order turns to COMPLETE
            and vice versa

        """
        try:
            sl_order_detail = self.kite.order_history(order_id=sl_orderid)[-1]
            tgt_order_detail = self.kite.order_history(order_id=tgt_orderid)[-1]

            if sl_order_detail['status'] == "CANCELLED" and tgt_order_detail['status'] == 'CANCELLED':
                
                raise ValueError("SL and Target order are already cancelled")
            
            if sl_order_detail['status'] == "COMPLETE" and tgt_order_detail['status'] != 'COMPLETE':

                if tgt_order_detail['status'] == "CANCELLED":
                    return "Target order already cancelled"
                
                print(f"Canceling Target order {tgt_orderid}")
                
                self.kite.cancel_order(variety = tgt_order_detail['variety'], order_id=tgt_orderid)

            if tgt_order_detail['status'] == "COMPLETE" and sl_order_detail['status'] != 'COMPLETE':

                if sl_order_detail['status'] == "CANCELLED":
                    return "Target order already cancelled"
                
                print(f"Canceling Stoploss order {sl_orderid}")
                
                self.kite.cancel_order(variety = sl_order_detail['variety'], order_id=sl_orderid)

        except Exception as e:
            print(e)
            traceback.print_exc()


    def position_sizing(self,name,number_of_quantity_to_be_exited,sl_order=None):
        try:
            """ This can be modified as per requirements, 
                This function will exit the positions and change the SL order quantity if any
                user need to specify the number quantity need to be exited
                user can also specify the sl order ID to modify SL order as per exited quantity
            """
            positions = pd.DataFrame(self.kite.positions()['net'])

            for index, row in positions.iterrows():

                if row['tradingsymbol'] == name:
                    
                    if row['quantity'] > 0:
                        if number_of_quantity_to_be_exited <= row['quantity']:
                            qty_to_exit = number_of_quantity_to_be_exited

                            exit_order_id = self.kite.place_order(
                                variety=self.kite.VARIETY_REGULAR,
                                exchange=row['exchange'], 
                                tradingsymbol=row['tradingsymbol'], 
                                transaction_type=self.kite.TRANSACTION_TYPE_SELL, 
                                quantity=qty_to_exit, 
                                product= row['product'],    
                                order_type=self.kite.ORDER_TYPE_MARKET)

                        if number_of_quantity_to_be_exited <= row['quantity']:
                            print("number_of_quantity_to_be_exited is greater than current positions")

                        if sl_order!= None:
                            sl_order_detail = self.kite.order_history(order_id=sl_order)[-1]
                            
                            if number_of_quantity_to_be_exited == row['quantity']:
                                if sl_order_detail['status'] == 'CANCELLED':
                                    return 'SL order already cancelled'

                                self.kite.cancel_order(variety = sl_order_detail['variety'], order_id=sl_order)

                            else:
                                sl_quantity = row['quantity'] - number_of_quantity_to_be_exited
                                self.kite.modify_order(variety=self.kite.VARIETY_REGULAR,order_id=sl_order,quantity=sl_quantity)

                            return {'exited_order_id':exit_order_id}


                    elif row['quantity'] < 0:
                    
                        if number_of_quantity_to_be_exited * -1 >= row['quantity']:
                            qty_to_exit = number_of_quantity_to_be_exited

                            exit_order_id = self.kite.place_order(
                                variety=self.kite.VARIETY_REGULAR,
                                exchange=row['exchange'], 
                                tradingsymbol=row['tradingsymbol'], 
                                transaction_type=self.kite.TRANSACTION_TYPE_BUY, 
                                quantity=qty_to_exit, 
                                product= row['product'],    
                                order_type=self.kite.ORDER_TYPE_MARKET)

                            if sl_order!= None:
                                sl_order_detail = self.kite.order_history(order_id=sl_order)[-1]
                                if sl_order_detail['quantity'] == number_of_quantity_to_be_exited:
                                    if sl_order_detail['status'] == 'CANCELLED':
                                        
                                        return 'SL order already cancelled'

                                    self.kite.cancel_order(variety = sl_order_detail['variety'], order_id=sl_order)

                                else:
                                    sl_quantity = sl_order_detail['quantity']  - number_of_quantity_to_be_exited

                                    self.kite.modify_order(variety=self.kite.VARIETY_REGULAR,order_id=sl_order,quantity=sl_quantity)

                            return {'exited_order_id':exit_order_id}

                    else:
                        return f"No running position for {name}"

        except Exception as e:
            print(e)
            traceback.print_exc()



    def send_telegram_alert(self,message,receiver_chat_id,bot_token=None):
        """
            1st receiver need to connect with BOT TradeHull Bot token is "5189311784:AAHgQxiQ6uhc1Qf7AvPAiUoUzxetu8uKP58" 
        """
        try:
            bot_token = "5189311784:AAHgQxiQ6uhc1Qf7AvPAiUoUzxetu8uKP58"
            send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + receiver_chat_id + '&text=' + message
            response = requests.get(send_text)
        except Exception as e:
            print(e)
            traceback.print_exc()



    def market_open_stock_movement(self,name,exchange):
        """
                this function will return gapup/gapdown percentage
        """
        try:
            df = self.get_short_length_hist_data(name=name,exchange=exchange,interval='day')

            yesterdays_close = df.iloc[-2]['close']
            todays_open = df.iloc[-1]['open']

            percentage_moves = ((todays_open - yesterdays_close)/yesterdays_close)*100

            return percentage_moves
        except Exception as e:
            print(e)
            traceback.print_exc()


    def place_order(self, variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type, validity, price=None, trigger_price=None,tag=None):
        """
            we have added wrapper if kite api didnt return the order id, we will check orderbook before and after order placement
        """

        try:
            p_orders = pd.DataFrame(self.kite.orders())
            before_len = len(p_orders)

            order_id = self.kite.place_order(variety=variety, exchange=exchange, tradingsymbol=tradingsymbol, transaction_type=transaction_type, quantity=quantity, product=product, order_type=order_type, price=price, validity=validity, trigger_price=trigger_price,tag=tag)
            time.sleep(1)
            c_orders = pd.DataFrame(self.kite.orders())
            after_len = len(c_orders)
            if order_id == None:
                print("didnt find order id from api trying to get it via wrapper")
                if before_len < after_len:
                    order_id = c_orders.iloc[-1]['order_id']
                    return order_id
            else:
                return order_id
        except Exception as e:
            traceback.print_exc()



    def modify_order(self, variety, order_id, parent_order_id=None, quantity=None, price=None, order_type=None, trigger_price=None, validity=None, disclosed_quantity=None):
        try:
            order_id = self.kite.modify_order(variety=variety, order_id=order_id, parent_order_id=parent_order_id, quantity=quantity, price=price, order_type=order_type, trigger_price=trigger_price, validity=validity, disclosed_quantity=disclosed_quantity)
            return order_id
        except Exception as e:
            print(e)
            traceback.print_exc()




    def break_even_logic_pseudocode():
        pass
        """snapshot_price = xyz

            if ltp> snapshot_price:
                place buy order
            if ltp < snapshot_price:
                place sell order"""

    # def download_instrument_file(self):
    #     instrument_dff = pd.DataFrame(self.kite.instruments())
    #     return instrument_dff
