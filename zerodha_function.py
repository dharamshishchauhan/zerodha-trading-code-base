import pandas as pd
import time
import mibian
import datetime

class indicator:

    def get_pivot_point(self, data, flag:str):
        self.params = {flag:str}
        res = self.check_if_parameter_is_correct(self.params)
        data['only_date'] = data['date'].dt.date
        data['only_date'] = pd.to_datetime(data['only_date'], format='%Y-%m-%d')
        day = self.get_last_tradable_day()
        data = data.loc[data['only_date'] == day]
        PP = (data['high'].max() + data['low'].min() + data["close"].iloc[-1])/3 
        R1 = 2 * PP - data['low'].min()
        R2 = PP + (data['high'].max() - data['low'].min())         
        R3 = PP + 2 * (data['high'].max() - data['low'].min())          
        S1 = 2 * PP - data['high'].max()          
        S2 = PP - (data['high'].max() - data['low'].min())
        S3 = PP - 2 * (data['high'].max() - data['low'].min()) 

        if flag == "pp":            
            return PP
        if flag == 'r1':
            return R1
        if flag == 'r2':  
            return S1
        if flag == 'r3':   
            return R2
        if flag == 's1':  
            return S2
        if flag == 's2':  
            return R3
        if flag == 's3':
            return S3
        

    def get_fibonacci_pivot_point(self,data,flag:str):
        self.params = {flag:str}
        res = self.check_if_parameter_is_correct(self.params)
        data['only_date'] = data['date'].dt.date
        data['only_date'] = pd.to_datetime(data['only_date'], format='%Y-%m-%d')
        day = self.get_last_tradable_day()
        data = data.loc[data['only_date'] == day]

        if flag == "pp":            
            value = (data['high'].max() + data['low'].min() + data["close"].iloc[-1])/3 
            return value

        if flag == 'r1':
            pivot = (data['high'].max() + data['low'].min() + data["close"].iloc[-1])/3 
            value = pivot + (.382 * (data['high'].max() -  data['low'].min())) 
            return value

        if flag == 'r2':            
            pivot = (data['high'].max() + data['low'].min() + data["close"].iloc[-1])/3 
            value = pivot + (.618 * (data['high'].max() -  data['low'].min()))
            return value

        if flag == 'r3':            
            pivot = (data['high'].max() + data['low'].min() + data["close"].iloc[-1])/3 
            value = pivot + (1 * (data['high'].max() -  data['low'].min()))
            return value

        if flag == 's1':            
            pivot = (data['high'].max() + data['low'].min() + data["close"].iloc[-1])/3 
            value = pivot - (.382 * (data['high'].max() -  data['low'].min()))  
            return value

        if flag == 's2':            
            pivot = (data['high'].max() + data['low'].min() + data["close"].iloc[-1])/3 
            value = pivot - (.618 * (data['high'].max() -  data['low'].min())) 
            return value

        if flag == 's3':            
            pivot = (data['high'].max() + data['low'].min() + data["close"].iloc[-1])/3 
            value = pivot - (1* (data['high'].max() -  data['low'].min()))
            return value
    
    def market_over_close_all_ordermis(self):
        try:
            # close pending trades
            orders = pd.DataFrame(self.kite.orders())
            if orders.empty:
                return

            trigger_pending_orders = orders.loc[(orders['status'] == 'TRIGGER PENDING') & (orders['product'] == 'MIS')]
            open_orders = orders.loc[(orders['status'] == 'OPEN') & (orders['product'] == 'MIS')]
            for index, row in trigger_pending_orders.iterrows():
                self.kite.cancel_order(variety=row['variety'], order_id=row['order_id'])

            for index, row in open_orders.iterrows():
                self.kite.cancel_order(variety=row['variety'], order_id=row['order_id'])


            position = pd.DataFrame(self.kite.positions()['net'])

            if position.empty :
                return

            positive_trades = position.loc[(position['quantity'] > 0) & (position['product'] == 'MIS')]

            negative_trades = position.loc[(position['quantity'] < 0) & (position['product'] == 'MIS')]

            for index, row in negative_trades.iterrows():
                quantity = row['quantity'] * -1
                ltp_negative=self.kite.ltp('NFO'+str(":")+row['tradingsymbol'])['NFO'+str(":")+row['tradingsymbol']]['last_price']
                self.kite.place_order(variety=self.kite.VARIETY_REGULAR, exchange=row['exchange'], tradingsymbol=row['tradingsymbol'],transaction_type=self.kite.TRANSACTION_TYPE_BUY, quantity=quantity, product=self.kite.PRODUCT_MIS, order_type=self.kite.ORDER_TYPE_LIMIT,price=round(ltp_negative/100*115,1))
                time.sleep(1)
            for index, row in positive_trades.iterrows():
                ltp_positive=self.kite.ltp('NFO'+str(":")+row['tradingsymbol'])['NFO'+str(":")+row['tradingsymbol']]['last_price']
                self.kite.place_order(variety=self.kite.VARIETY_REGULAR, exchange=row['exchange'], tradingsymbol=row['tradingsymbol'],transaction_type=self.kite.TRANSACTION_TYPE_SELL, quantity=row['quantity'], product=self.kite.PRODUCT_MIS, order_type=self.kite.ORDER_TYPE_LIMIT,price=round(ltp_positive/100*85,1))
                time.sleep(1)

        except Exception as e:
            print(e)

    def get_implied_volatility(self, strike:int, expiry_date:str, asset:str, interest_rate:float, scrip_type:str):
        self.params = {strike:int, expiry_date:str, asset:str, interest_rate:int, scrip_type:str}
        res = self.check_if_parameter_is_correct(self.params)
        
        if asset == 'NIFTY BANK':
            inst_asset = "BANKNIFTY"
        if asset == 'NIFTY 50':
            inst_asset = "NIFTY"
        try:
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
                return civ.impliedVolatility
            if scrip_type == "PE":
                piv = mibian.BS([asset_price, strike, interest_rate, days_to_expiry], putPrice= ltp)
                return piv.impliedVolatility
        
        except Exception as e:
            pass
              
    def get_cross_over_signal(cross_df,indi_candle,name):
        """
            this code is not for direct use from library.. this is the references to take while generating crossover signal
            # fast_col - 1st indicator data
            # slow_col - 2nd indicator data
            # PCO - Positive crossover
            # NCO - Negative crossover
            # POSITIVE - indicator already crossed in positive direction- and its continue to positive
            # NEGATIVE - indicator already crossed in negative direction- and its continue to negative
        """
        current_fast = cross_df.iloc[indi_candle]['fast_col']
        current_slow = cross_df.iloc[indi_candle]['slow_col']
        previous_fast = cross_df.iloc[indi_candle-1]['fast_col']
        previous_slow = cross_df.iloc[indi_candle-1]['slow_col']

        if previous_fast < previous_slow and current_fast > current_slow:
            return 'PCO'
        elif previous_fast > previous_slow and current_fast < current_slow:
            return 'NCO'
        elif current_fast < current_slow:
            return 'NEGATIVE'
        elif current_fast > current_slow:
            return 'POSITIVE'
