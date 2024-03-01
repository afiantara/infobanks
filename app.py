import os
import pandas as pd
import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from log import *

data_folder = './Data'
db_name_asuransi_jiwa='ASURANSI'
db_name_asuransi_umum='ASURANSI_UMUM'

months  =['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember']

def get_report_date(year,month):
    my_month = months.index(month) + 1
    cons_tanggal ="{}-{:02}".format(year,my_month)
    final_tanggal = datetime.strptime(cons_tanggal, "%Y-%m")
    report_date = final_tanggal - relativedelta(day=31)
    return report_date.date()

def dump_to_db(db_name,table_name,df,report_date):
    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError
    db = create_engine('sqlite:///{}.db'.format(db_name))
    # drop 'records table'
    try:
        with db.connect() as conn:
            query="DELETE FROM {} WHERE REPORT_DATE='{}'".format(table_name,report_date)
            r_set=conn.exec_driver_sql(query)
    except SQLAlchemyError as e:
        error = str(e)
        print(error)
    else:
        print("No of Records deleted : ",r_set.rowcount)
    finally:
        df.to_sql(table_name, db, if_exists='append')

def get_columns(dbname,tablename):
    import pandas as pd
    import sqlite3
    # Read sqlite query results into a pandas DataFrame
    con = sqlite3.connect('{}.db'.format(dbname))
    df = pd.read_sql_query('SELECT * from {} limit 0'.format(tablename), con)
    return df

def investasi(df,report_date,isJiwa=True):
    table_name='INVESTASI'

    columns=['REPORT_DATE','NAMA PERUSAHAAN', 
    'DEPOSITO BERJANGKA',
    'SAHAM', 
    'OBLIGASI DAN MTN',
    'SB. YANG DITERBITKAN NEGARA RI',
    'SB. YANG DITERBITKAN SELAIN NEGARA RI',
    'SB. YANG DITERBITKAN BANK INDONESIA',
    'SURAT BERHARGA YANG DITERBITKAN LEMBAGA MULTINASIONAL', 
    'REKSADANA',
    'KONTRAK INVESTASI KOLEKTIF EBA', 
    'DANA INVESTASI REAL ESTAT', 
    'PENYERTAAN LANGSUNG', 
    'TANAH, BANGUNAN, TANAH DNG BANGUNAN',
    'PEMBELIAN PIUTANG UNTUK PERUSAHAAN PEMBIAYAAN DAN/ATAU BANK',
    'EXECUTING', 
    'EMAS MURNI', 
    'PINJAMAN YANG DIJAMIN HAK TANGGUNGAN',
    'PINJAMAN POLIS', 
    'INVESTASI LAIN',
    'JUMLAH INVESTASI','PRICE_INDEX']
    df.columns = columns
    df = df[df['NAMA PERUSAHAAN'].notna()]
    df=df.dropna(subset=['JUMLAH INVESTASI'])
    df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    dump_to_db(db_name_asuransi_jiwa,table_name,df,report_date)

def non_investasi(df,report_date,isJiwa=True):
    table_name='NONINVESTASI'
    columns=['REPORT_DATE','NAMA PERUSAHAAN', 
    'KAS DAN BANK    ',
    'TAGIHAN/ PIUTANG PREMI PENUTUPAN  LANGSUNG  ',
    'TAGIHAN/ PIUTANG PREMI REASURANSI   ',
    'TAGIHAN/ PIUTANG KLAIM KOASURANSI  ',
    'TAGIHAN/ PIUTANG KLAIM REASURANSI  ',
    'TAGIHAN/ PIUTANG HASIL INVESTASI   ',
    'TAGIHAN/ PIUTANG INVESTASI   ',
    'TAGIHAN/ PIUTANG LAIN-LAIN   ',
    'ASET REASURANSI    ',
    'PINJAMAN POLIS    ',
    'BANGUNAN DENGAN HAK STRATA ATAU TANAH DENGAN BANGUNAN UNTUK DIPAKAI SENDIRI',
    'PERANGKAT KERAS KOMPUTER   ',
    'BIAYA DIBAYAR DIMUKA   ',
    'PAJAK DIBAYAR DIMUKA   ',
    'AKTIVA PAJAK TANGGUHAN   ',
    'AKTIVA TIDAK BERWUJUD   ',
    'AKTIVA TETAP LAIN   ',
    'AKTIVA LAIN    ',
    'JUMLAH BUKAN INVESTASI','PRICE_INDEX']
    df.columns = columns
    df = df[df['NAMA PERUSAHAAN'].notna()]
    df=df.dropna(subset=['JUMLAH BUKAN INVESTASI'])
    df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    print(df)
    dump_to_db(db_name_asuransi_jiwa,table_name,df,report_date)

def read_data(filename):
    f = os.path.join(data_folder, filename)
    df = pd.read_excel(f)

    arr_file = filename.split('-')
    
    year = int(arr_file[0][-4:])
    
    sector =''
    if 'jiwa' in arr_file[0].lower():
        sector='asuransi jiwa'
    else:
        sector = 'asuransi umum'
    info('{}-{}'.format(year,sector))
    df = df.iloc[15:,2:]
    report_date=get_report_date(year,'Desember')

    if sector=='asuransi jiwa':
        dfinvestasi=df.iloc[:,:20]
        dfinvestasi['PRICE_INDEX']=1000000
        dfinvestasi.insert(0,'REPORT_DATE',report_date)
        #INVESTASI
        investasi(dfinvestasi,report_date)

        df_company=df.iloc[:,:1]
        df_company.columns=['PERUSAHAAN ASURANSI']
        dfnoninvestasi = df.iloc[:,22:41]
        dfnoninvestasi=pd.concat([df_company,dfnoninvestasi],axis=1)
        dfnoninvestasi['PRICE_INDEX']=1000000
        dfnoninvestasi.insert(0,'REPORT_DATE',report_date)
        #NONINVESTASI
        non_investasi(dfnoninvestasi,report_date)
        #UTANG
    else:
        investasi(df,False)
def init():
    info('init')
    for root_dir, cur_dir, files in os.walk(data_folder):
        for file in files:
            info(file)
            read_data(file)
            break

if __name__=="__main__":
    loginit()
    init()
