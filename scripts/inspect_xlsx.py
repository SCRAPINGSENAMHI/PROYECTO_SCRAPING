import sys
import pandas as pd
p = r'G:/1_PROYECTOS/WEBSCRAPING/Web_Scraping_SENAMHI_/DATA/outputs/0000_ALAMOR_104044.xlsx'
try:
    df = pd.read_excel(p)
    print('ROWS', len(df))
    print(df.to_string(index=False))
except Exception as e:
    print('ERROR', e)
    sys.exit(1)
