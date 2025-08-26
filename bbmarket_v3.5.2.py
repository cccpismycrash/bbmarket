import numpy as np
import pandas as pd
from datetime import datetime

from tenacity import retry, stop_after_delay, wait_fixed
from httpx import Client, QueryParams, Response
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_delay, wait_fixed

class TryMoexDataLoader:
    def __init__(self) -> None:
        self._http_client = Client()

    @retry(wait=wait_fixed(5), stop=stop_after_delay(10), reraise=True)
    def _request(self, url: str, params: QueryParams | None = None) -> Response:
        response = self._http_client.get(url, params=params)
        return response
    
    def _get_candles(self, _ticker: str, _from: str, till: str, interval: int = 24) -> pd.DataFrame:

        url = f'https://iss.moex.com/iss/engines/stock/markets/index/securities/{_ticker}/candles'

        params = {
            'from':_from, 
            'till': till, 
            'interval': interval
            }

        http = self._request(url, params)
        soup = BeautifulSoup(http.text, 'xml')

        res_list = list()
        headers = list()
        for header in soup.find('metadata').find_all('column'):
            headers.append(header['name'])
        
        for row in soup.find('rows').find_all('row'):
            _table_row = dict()
            for header in headers:
                _table_row[header] = row[header]
            res_list.append(_table_row)

        if not res_list:
            return None

        else:
            df = pd.json_normalize(res_list)

            df.rename(columns={"begin": "date"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df.set_index("date", inplace=True)
            df.loc[:, df.columns != "end"] = df.loc[:, df.columns != "end"].apply(pd.to_numeric, errors="coerce") 

        return df
    
    
    
    def get_ticker(self, _ticker: str, _from: str, till: str, interval: int = 24) -> pd.DataFrame:
        
        df_old = self._get_candles(_ticker, _from, till, interval)
        
        if df_old is not None and not df_old.empty:

            date_end = df_old.index[-1]
            
            while True:
                df_append = self._get_candles(_ticker, date_end, till, interval)

                if df_append is not None and not df_append.empty and df_append.index[-1] != df_old.index[-1]:
                    df_appended = pd.concat([df_old, df_append], axis=0)
                    date_end = df_appended.index[-1]
                    df_old = df_appended
                else:
                    break

            df_old.drop_duplicates(inplace=True)
        
            return df_old
        
        else:
            return None

_FROM = None
THRESHOLD = 0.2
DAYS_FILTER = 60

loader = TryMoexDataLoader()

df = loader.get_ticker('IMOEX', _FROM, None)

df.reset_index(inplace=True)

df.rename(columns={'date':'time'}, inplace=True)

df['time'] = pd.to_datetime(df['time'])

list_of_bull_markets = list()
list_of_bear_markets = list()

start_per = None
bear_mode = False
bull_mode = False
suspicious_bull_extr = 0
suspicious_bear_extr = 0

for i in range(len(df)):
    if i == 0:
        current_row = df.iloc[i]
        previous_row = df.iloc[i]
        bear_basis = current_row['high']
        bull_basis = current_row['low']
        start_bull_per = current_row['time']
        start_bear_per = current_row['time']
        actual_growth = 0
    else:
        current_row = df.iloc[i]
        previous_row = df.iloc[i-1]

    bear_growth = current_row['low'] / bear_basis - 1
    bull_growth = current_row['high'] / bull_basis - 1


#  
    if (not bear_mode) & (not bull_mode) & (bull_growth > THRESHOLD) & ((current_row['time'] - start_bull_per).days > DAYS_FILTER):
        bull_mode = True
#  
    if (not bear_mode) & (not bull_mode) & (bear_growth < -THRESHOLD) & ((current_row['time'] - start_bear_per).days > DAYS_FILTER):
        bear_mode = True

    if bull_mode:
        if ((bull_growth >= suspicious_bull_extr) & 
            (bull_growth >= THRESHOLD)):
            
            suspicious_bull_extr = bull_growth
            suspicious_bull_end = current_row['time']
            bear_basis = current_row['high']
            suspicious_bear_extr = 0
            start_bear_per = current_row['time']

        elif bear_growth <= -THRESHOLD:
            if (suspicious_bull_end - start_bull_per).days >= DAYS_FILTER:
                end_bull_per = suspicious_bull_end
                end_bull_val = bear_basis
                bull_mode = False
                bear_mode = True
                
                print(f'''
                        Начало бычьего рынка: {start_bull_per}\n
                        Конец бычьего рынка: {end_bull_per}\n
                        Low в начале: {bull_basis}\n
                        High в конце: {end_bull_val}\n
                        Рост за период: {suspicious_bull_extr}\n
                        ''')
                print('\n')

                list_of_bull_markets.append({
                'start_period': start_bull_per,
                'end_period': end_bull_per,
                'low_start': bull_basis,
                'high_end': end_bull_val,
                'duration': (end_bull_per - start_bull_per).days,
                'growth %': np.round(suspicious_bull_extr * 100, 2)
                })
                
                last_item = list_of_bull_markets.pop()
                aux_df = df[(df['time'] >= last_item['start_period']) & (df['time'] <= last_item['end_period'])]
                aux_row = aux_df.loc[aux_df['high'].idxmax()]

                if (aux_row['time'] - start_bull_per).days >= DAYS_FILTER:

                    list_of_bull_markets.append({
                    'start_period': start_bull_per,
                    'end_period': aux_row['time'],
                    'low_start': bull_basis,
                    'high_end': aux_row['high'],
                    'duration': (aux_row['time'] - start_bull_per).days,
                    'growth %': np.round((aux_row['high'] / bull_basis - 1) * 100, 2)
                    })

                    bear_basis = aux_row['high']
                    suspicious_bear_extr = 0
                    start_bear_per = aux_row['time']
                
                else:
                    list_of_bull_markets.append(last_item)

            else:
                bear_popped_row = list_of_bear_markets.pop()
                start_bear_per = bear_popped_row['start_period']
                bear_basis = bear_popped_row['high_start']
                bull_mode = False
                bear_mode = True

            if ((bear_growth <= suspicious_bear_extr) & 
                (bear_growth <= -THRESHOLD)):

                suspicious_bear_extr = bear_growth
                suspicious_bear_end = current_row['time']
                bull_basis = current_row['low']
                suspicious_bull_extr = 0
                start_bull_per = current_row['time']

        else: 
            pass


    if bear_mode:
        if ((bear_growth <= suspicious_bear_extr) & 
            (bear_growth <= -THRESHOLD)):

            suspicious_bear_extr = bear_growth
            suspicious_bear_end = current_row['time']
            bull_basis = current_row['low']
            suspicious_bull_extr = 0
            start_bull_per = current_row['time']
            
        elif bull_growth >= THRESHOLD:
            if (suspicious_bear_end - start_bear_per).days >= DAYS_FILTER:
                end_bear_per = suspicious_bear_end
                end_bear_val = bull_basis
                bear_mode = False
                bull_mode = True

                print(f'''
                        Начало медвежьего рынка: {start_bear_per}\n
                        Конец медвежьего рынка: {end_bear_per}\n
                        High в начале: {bear_basis}\n
                        Low в конце: {end_bear_val}\n
                        Рост за период: {suspicious_bear_extr}\n
                        ''')
                print('\n')

                list_of_bear_markets.append({
                'start_period': start_bear_per,
                'end_period': end_bear_per,
                'high_start': bear_basis,
                'low_end': end_bear_val,
                'duration': (end_bear_per - start_bear_per).days,
                'growth %': np.round(suspicious_bear_extr * 100, 2)
                })

                last_item = list_of_bear_markets.pop()
                aux_df = df[(df['time'] >= last_item['start_period']) & (df['time'] <= last_item['end_period'])]
                aux_row = aux_df.loc[aux_df['low'].idxmin()]

                if (aux_row['time'] - start_bear_per).days >= DAYS_FILTER:

                    list_of_bear_markets.append({
                    'start_period': start_bear_per,
                    'end_period': aux_row['time'],
                    'high_start': bear_basis,
                    'low_end': aux_row['low'],
                    'duration': (aux_row['time'] - start_bear_per).days,
                    'growth %': np.round((aux_row['low'] / bear_basis - 1) * 100, 2)
                    })

                    bull_basis = aux_row['low']
                    suspicious_bull_extr = 0
                    start_bull_per = aux_row['time']

                else:

                    list_of_bear_markets.append(last_item)


            else:
                bull_popped_row = list_of_bull_markets.pop()
                start_bull_per = bull_popped_row['start_period']
                bull_basis = bull_popped_row['low_start']
                bear_mode = False
                bull_mode = True

            if ((bull_growth >= suspicious_bull_extr) & 
                (bull_growth >= THRESHOLD)):
                
                suspicious_bull_extr = bull_growth
                suspicious_bull_end = current_row['time']
                bear_basis = current_row['high']
                suspicious_bear_extr = 0
                start_bear_per = current_row['time']

        else: 
            pass

if bull_mode:
    print(f'''
                    Начало бычьего рынка: {start_bull_per}\n
                    Конец бычьего рынка: {suspicious_bull_end}\n
                    Low в начале: {bull_basis}\n
                    High в конце: {bear_basis}\n
                    Рост за период: {suspicious_bull_extr}\n
            ''')
    print('\n')

    list_of_bull_markets.append({
    'start_period': start_bull_per,
    'end_period': suspicious_bull_end,
    'low_start': bull_basis,
    'high_end': bear_basis,
    'duration': (suspicious_bull_end - start_bull_per).days,
    'growth %': np.round(suspicious_bull_extr * 100, 2)
    })

if bear_mode:
    print(f'''
                        Начало медвежьего рынка: {start_bear_per}\n
                        Конец медвежьего рынка: {suspicious_bear_end}\n
                        High в начале: {bear_basis}\n
                        Low в конце: {bull_basis}\n
                        Рост за период: {suspicious_bear_extr}\n
            ''')
    print('\n')

    list_of_bear_markets.append({
    'start_period': start_bear_per,
    'end_period': suspicious_bear_end,
    'high_start': bear_basis,
    'low_end': bull_basis,
    'duration': (suspicious_bear_end - start_bear_per).days,
    'growth %': np.round(suspicious_bear_extr * 100, 2)
    })


df_bull_market = pd.json_normalize(list_of_bull_markets)
df_bear_market = pd.json_normalize(list_of_bear_markets)

df_bull_market.to_csv(f'bull_markets_{DAYS_FILTER}_{_FROM}.csv', index=False)
df_bear_market.to_csv(f'bear_markets_{DAYS_FILTER}_{_FROM}.csv', index=False)
