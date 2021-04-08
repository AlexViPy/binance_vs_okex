import os
import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv

binance_trades_data = []
okex_trades_data = []

# loading the library dotenv to hide binance api key
load_dotenv(find_dotenv())


class Client:
    def __init__(self):
        self.headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) '
                                      'Version/14.0.1 Safari/605.1.15 ',
                        'X-MBX-APIKEY': os.getenv('BINANACE_APIKEY')}

        self.urls = ['https://api.binance.com/api/v3/exchangeInfo',
                     'https://api.binance.com/api/v3/trades',
                     'https://www.okex.com/api/spot/v3/instruments']

    # sending a request to the server
    def load_page(self, url=''):
        response = requests.Session().get(url=url, headers=self.headers)
        response.raise_for_status()
        return response

    # unloading Binance tools
    def get_pairs_without_binance(self):
        pairs = self.load_page(url=self.urls[0]).json()
        pairs_data = [item['symbol'] for item in pairs['symbols']]
        return pairs_data

    # unloading Okex tools
    def get_instruments_without_okex(self):
        instruments = self.load_page(url=self.urls[2]).json()
        instruments_data = [item['instrument_id'] for item in instruments]
        return instruments_data

    # uploading all trades to Binance
    def get_binance_trades(self):
        for pair in self.get_pairs_without_binance():
            trades = self.load_page(f'{self.urls[1]}?symbol={pair}').json()
            for trade in trades:
                binance_trades_data.append(trade)
        return binance_trades_data

    # uploading all trades to Okex
    def get_okex_trades(self):
        for instrument in self.get_instruments_without_okex():
            instrument_trades = self.load_page(f'{self.urls[2]}/{instrument}/ticker').json()
            okex_trades_data.append(instrument_trades)
        return okex_trades_data

    # converting Binance trades to df
    def change_binance_data_to_df(self):
        binance_data = pd.DataFrame(binance_trades_data)
        return binance_data

    # converting Okex trades to df
    def change_okex_data_to_df(self):
        okex_data = pd.DataFrame(okex_trades_data)
        return okex_data

    # converting Okex tools to df
    def change_instruments_without_okex_to_df(self):
        instruments_data = [item.replace('-', '') for item in self.get_instruments_without_okex()]
        instruments_data = pd.DataFrame(instruments_data)
        return instruments_data

    # converting Binance tools to df
    # identifying dissimilar tools
    def change_pairs_without_binance_to_df(self):
        pairs_binance = pd.DataFrame(self.get_pairs_without_binance())
        diff = pd.concat([self.change_instruments_without_okex_to_df(), pairs_binance],
                         ignore_index=True).drop_duplicates(keep=False)
        pairs_binance.rename(columns={0: 'binance'}, inplace=True)
        pairs_binance['okex'] = self.change_instruments_without_okex_to_df()[0]
        pairs_binance['diff'] = diff[0]
        return pairs_binance

    # save the results to an .xlsx file
    def save_to_file(self):
        self.change_binance_data_to_df().to_excel('exchanges.xlsx', sheet_name='binance_data')

        with pd.ExcelWriter('exchanges.xlsx', engine='openpyxl', mode='a') as writer:
            self.change_okex_data_to_df().to_excel(writer, sheet_name='okex_data')
            self.change_pairs_without_binance_to_df().to_excel(writer, sheet_name='diff')
            writer.save()


if __name__ == '__main__':
    client = Client()
    client.get_pairs_without_binance()
    client.get_instruments_without_okex()
    client.get_binance_trades()
    client.get_okex_trades()
    client.change_binance_data_to_df()
    client.change_okex_data_to_df()
    client.change_instruments_without_okex_to_df()
    client.change_pairs_without_binance_to_df()
    client.save_to_file()
