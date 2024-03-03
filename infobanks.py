import os
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from log import *

data_folder = './Data'
db_name_asuransi_jiwa='ASURANSI'
db_name_asuransi_umum='ASURANSI_UMUM'

db_name_kinerja='KINERJA'

months  =['Januari','Februari','Marpythonet','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember']

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
            print(query)
            r_set=conn.exec_driver_sql(query)
            conn.commit()
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
    #print(df)
    dump_to_db(db_name_asuransi_jiwa,table_name,df,report_date)

def utang(df,report_date,isJiwa=True):
    table_name='UTANG'
    columns=['REPORT_DATE','NAMA PERUSAHAAN', 
    'HUTANG KLAIM ',
    'TITIPAN PREMI ',
    'HUTANG KOASURANSI ',
    'HUTANG REASURANSI ',
    'HUTANG KOMISI ',
    'HUTANG PAJAK ',
    'BIAYA YANG MASIH HARUS DIBAYAR',
    'HUTANG ZAKAT ',
    'KEWAJIBAN IMBALAN PASCA KERJA',
    'HUTANG LAIN ',
    'JUMLAH UTANG ','PRICE_INDEX']
    
    df.columns = columns
    df = df.rename(columns=lambda x: x.strip()) #membuang space di dalam kolom
    
    df = df[df['NAMA PERUSAHAAN'].notna()]
    df=df.dropna(subset=['JUMLAH UTANG'])
    df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    #print(df)
    dump_to_db(db_name_asuransi_jiwa,table_name,df,report_date)
        
def cadangan_teknis(df,report_date,isJiwa=True):
    table_name='CADANGANTEKNIS'
    columns=['REPORT_DATE','NAMA PERUSAHAAN', 
    'CADANGAN PREMI (KEWAJIBAN MANFAAT POLIS MASA DEPAN) ',
    'CADANGAN PREMI (KEWAJIBAN PRODUK UNIT LINK PIHAK KETIGA) ',
    'PENYISIHAN KONTRIBUSI   ',
    'DANA TABARRU   ',
    'TABUNGAN PESERTA   ',
    'PENYISIHAN KONTRIBUSI YANG BELUM MEN- JADI PENDAPATAN ',
    'PENYISIHAN KLAIM   ',
    'CADANGAN ATAS PREMI YANG BELUM  MERUPAKAN PENDAPATAN',
    'CADANGAN KLAIM (ESTIMASI KEWAJIBAN KLAIM)',
    'CADANGAN ATAS RISIKO BENCANA (CATASTROPHIC)',
    'JUMLAH CADANGAN TEKNIS  ',
    'PRICE_INDEX']
    
    df.columns = columns
    df = df.rename(columns=lambda x: x.strip()) #membuang space di dalam kolom
    
    df = df[df['NAMA PERUSAHAAN'].notna()]
    df=df.dropna(subset=['JUMLAH CADANGAN TEKNIS'])
    df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    
    #print(df)
    dump_to_db(db_name_asuransi_jiwa,table_name,df,report_date)

def liabilitas(df,report_date,isJiwa=True):
    table_name='KEWAJIBAN'
    columns=['REPORT_DATE','NAMA PERUSAHAAN', 
    'AKUMULASI SURPLUS DANA TABARRU',
    'PINJAMAN SUBORDINASI  ',
    'LIABILITIS KEPADA PEMEGANG UNIT LINK',
    'HAK PEMEGANG SAHAM MINORITAS ',
    'KEWAJIBAN JANGKA PANJANG ',
    'PRICE_INDEX']
    
    df.columns = columns
    df = df.rename(columns=lambda x: x.strip()) #membuang space di dalam kolom
    df = df[df['NAMA PERUSAHAAN'].notna()]
    df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    df.drop(df.tail(7).index,inplace=True) # drop last n rows
    #print(df.tail(20))
    dump_to_db(db_name_asuransi_jiwa,table_name,df,report_date)

def modal_sendiri(df,report_date,isJiwa=True):
    table_name='MODAL_SENDIRI'
    columns=['REPORT_DATE','NAMA PERUSAHAAN', 
    'MODAL DISETOR  ',
    'SETORAN MODAL LAINNYA ',
    'AGIO SAHAM  ',
    'CADANGAN   ',
    'KENAIKAN (PENURUNAN) INVESTASI YANG BELUM DIREALISASI',
    'KENAIKAN (PENURUNAN) SURAT BERHARGA',
    'SELISIH NILAI TRANSAKSI RESTRUK- TURISASI ENTITAS SEPENGENDALI',
    'SELISIH PENILAIAN AKTIVA TETAP',
    'SAHAM DIPEROLEH KEMBALI ',
    'PENDAPATAN KOMPREHENSIF LAINNYA ',
    'KOMPONEN EKUITAS LAINNYA ',
    'SALDO LABA  ',
    'JUMLAH MODAL SENDIRI ',
    'PRICE_INDEX']
    
    df.columns = columns
    df = df.rename(columns=lambda x: x.strip()) #membuang space di dalam kolom
    
    df = df[df['NAMA PERUSAHAAN'].notna()]
    df=df.dropna(subset=['JUMLAH MODAL SENDIRI'])
    df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    
    #print(df)
    dump_to_db(db_name_asuransi_jiwa,table_name,df,report_date)

def pendapatan(df,report_date,isJiwa=True):
    table_name='PENDAPATAN'
    columns=['REPORT_DATE','NAMA PERUSAHAAN', 
    'PERUBAHAN PENDAPATAN  PREMI BRUTO (%)',
    'PENDAPATAN  PREMI BRUTO PREV',
    'PENDAPATAN  PREMI BRUTO  ',
    'PREMI REASURANSI   ',
    'PENURUNAN (KENAIKAN) PYBMP  ',
    'PYBMP AWAL TAHUN (TH. LALU)',
    'PYBMP AKHIR TAHUN (TH. BERJALAN)',
    'JUMLAH PENDAPATAN  PREMI NETO ',
    'JUMLAH PENDAPATAN  PREMI NETO PREV',
    'HASIL INVESTASI   ',
    'IMBALAN JASA DPLK/JASA MANAJEMEN LAINNYA ',
    'PENDAPATAN  LAIN   ',
    'JUMLAH PENDAPATAN    ',
    'PRICE_INDEX']
    
    df.columns = columns
    df = df.rename(columns=lambda x: x.strip()) #membuang space di dalam kolom
    
    df = df[df['NAMA PERUSAHAAN'].notna()]
    df=df.dropna(subset=['JUMLAH PENDAPATAN'])
    df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    
    #print(df)
    dump_to_db(db_name_asuransi_jiwa,table_name,df,report_date)
    
def beban(df,report_date,isJiwa=True):
    table_name='BEBAN'
    columns=['REPORT_DATE','NAMA PERUSAHAAN', 
    'KLAIM DAN MANFAAT DIBAYAR ',
    'KLAIM PENEBUSAN UNIT ',
    'KLAIM REASURANSI  ',
    'KENAIKAN (PENURUNAN) KMPMD ',
    '  KMPMD TH. BERJALAN',
    '  KMPMD TH. LALU',
    'KENAIKAN (PENURUNAN) DANA TABARRU',
    '  DANA TABARRU TH. BERJALAN',
    '  DANA TABARRU TH. LALU',
    'KENAIKAN (PENURUNAN) TABUNGAN PESERTA',
    '  TAB. PESERTA TH. BERJALAN',
    '  TAB. PESERTA TH. LALU',
    'KENAIKAN (PENURUNAN) EKK ',
    '  EKK TH. BERJALAN',
    '  EKK TH. LALU',
    'KENAIKAN (PENURUNAN) CADANGAN ATAS RISIKO',
    'BEBAN KLAIM DAN MANFAAT',
    'UNTUK FOKUS I  ',
    'BEBAN KOMISI - TAHUN PERTAMA',
    'BEBAN KOMISI - TAHUN LANJUTAN',
    'BEBAN KOMISI - OVERIDING ',
    'BEBAN LAINNYA  ',
    'BIAYA AKUISISI  ',
    'PRICE_INDEX']
    
    df.columns = columns
    df = df.rename(columns=lambda x: x.strip()) #membuang space di dalam kolom
    
    df = df[df['NAMA PERUSAHAAN'].notna()]
    df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    
    #print(df)
    dump_to_db(db_name_asuransi_jiwa,table_name,df,report_date)

def read(filename,rowstart=15,colstart=2,rowend=146):
    f = os.path.join(data_folder, filename)
    df = pd.read_excel(f)
    df = df.iloc[rowstart:rowend,colstart:]
    return df    

def year_sector(filename):
    arr_file = filename.split('-')
    year = int(arr_file[0][-4:])
    sector =''
    if 'jiwa' in arr_file[0].lower():
        sector='asuransi jiwa'
    else:
        sector = 'asuransi umum'
    return year,sector

def read_all_data_asuransi_umum(filename,year):
    
    if year==2022:
        df=read(filename,13,2,98)
        loc=[205]
        df=drop_col(df,loc)
    elif year==2021:
        df=read(filename,12,1,98)
        locs=[38,53,60,114,151,173]
        df=insert_col(df,locs)
        df=insert_col_range(df,175,205)
    elif year==2020:
        df=read(filename,13,1,102)
        locs=[150]
        df=insert_col(df,locs)
    elif year==2019:
        df=read(filename,12,1,98)
        locs=[38,53,60,114,150]
        df=insert_col(df,locs)
        df=insert_col_range(df,175,205)
    elif year==2018:
        df=read(filename,12,1,98)
        locs=[14,150]
        df=insert_col(df,locs)
        df=insert_col_range(df,175,205)
    elif year==2017:
        df=read(filename,16,1,101)
        locs=[14,150]
        df=insert_col(df,locs)
        df=insert_col_range(df,175,205)
    elif year==2016:
        df=read(filename,12,1,95)
        locs=[14,23,26,98,135,150]
        df=insert_col(df,locs)
        df=insert_col_range(df,172,205)
    elif year==2015:
        df=read(filename,12,1,96)
        locs=[14,23,26,98,135,150,171]
        df=insert_col(df,locs)
        df=insert_col_range(df,172,205)
        
    columns=[
        #'REPORT_DATE',
        'NAMA_PERUSAHAAN',
        'INVESTASI_DEPOSITO_BERJANGKA_DAN_SERTIFIKAT_DEPOSITO',
        'INVESTASI_SAHAM',
        'INVESTASI_OBLIGASI_MTN_DAN_SUKUK',
        'INVESTASI_SURAT_BERHARGA_YG_DITERBITKAN_ATAU_DIJAMIN_OLEH_NEGARA_RI',
        'INVESTASI_SURAT_BERHARGA_YG_DITERBITKAN_ATAU_DIJAMIN_OLEH_SELAIN_NEGARA_RI',
        'INVESTASI_SURAT_BERHARGA_YG_DITERBITKAN_ATAU_DIJAMIN_OLEH_BI',
        'INVESTASI_SURAT_BERHARGA_YG_DITERBITKAN_OLEH_LEMBAGA_MULTINASIONAL',
        'INVESTASI_UNIT_PENYERTAAN_REKSADANA',
        'INVESTASI_KONTRAK_INVESTASI_KOLEKTIF_EFEK_BERAGUN_ASET_(KIK_EBA)',
        'INVESTASI_DANA_INVESTASI_REAL_ESTAT',
        'INVESTASI_PENYERTAAN_LANGSUNG',
        'INVESTASI_BANGUNAN_DENGAN_HAK_STRATA_SATU_ATAU_TANAH_DG_BANGUNAN_UNTUK_INVESTASI',
        'INVESTASI_PEMBELIAN_PIUTANG_UNTUK_PERUSAHAAN_PEMBIAYAAN_DAN/ATAU_BANK',
        'INVESTASI_PEMBIAYAAN_MELALUI_KERJASAMA_DG_PIHAK_LAIN_(EXECUTING)',
        'INVESTASI_EMAS_MURNI',
        'INVESTASI_PINJAMAN_YANG_DIJAMIN_DENGAN_HAK_TANGGUNGAN_(PINJAMAN_HIPOTIK)',
        'INVESTASI_INVESTASI_LAIN',
        'INVESTASI_JUMLAH_INVESTASI',
        'INVESTASI_JUMLAH_INVESTASI_PREV',
        'INVESTASI_RATA-RATA_INVESTASI',
        'BUKAN_INVESTASI_KAS_DAN_BANK',
        'BUKAN_INVESTASI_TAGIHAN_PREMI_PENUTUPAN_LANGSUNG',
        'BUKAN_INVESTASI_TAGIHAN_PREMI_REASURANSI',
        'BUKAN_INVESTASI_TAGIHAN_KLAIM_KOASURANSI',
        'BUKAN_INVESTASI_TAGIHAN_KLAIM_REASURANSI',
        'BUKAN_INVESTASI_TAGIHAN_INVESTASI',
        'BUKAN_INVESTASI_TAGIHAN_HASIL_INVESTASI',
        'BUKAN_INVESTASI_BIAYA_DIBAYAR_DIMUKA',
        'BUKAN_INVESTASI_PAJAK_DIBAYAR_DIMUKA',
        'BUKAN_INVESTASI_PIUTANG_LAIN-LAIN',
        'BUKAN_INVESTASI_ASET_REASURANSI',
        'BUKAN_INVESTASI_BANGUNAN_DENGAN_HAK_STRATA_SATU_ATAU_TANAH_DG_BANGUNAN_UNTUK_DIPAKAI_SENDIRI',
        'BUKAN_INVESTASI_PERANGKAT_KERAS_KOMPUTER',
        'BUKAN_INVESTASI_ASET_PAJAK_TANGGUHAN',
        'BUKAN_INVESTASI_AKTIVA_TETAP',
        'BUKAN_INVESTASI_AKTIVA_LAIN',
        'BUKAN_INVESTASI_JUMLAH_BUKAN_INVESTASI',
        'BUKAN_INVESTASI_JUMLAH_BUKAN_INVESTASI_PREV',
        'AKTIVA_LANCAR',
        'TOTAL_ASET',
        'TOTAL_ASET_PREV',
        'UTANG_UTANG_KLAIM',
        'UTANG_UTANG_KOASURANSI',
        'UTANG_UTANG_REASURANSI',
        'UTANG_UTANG_KOMISI',
        'UTANG_UTANG_PAJAK',
        'UTANG_BIAYA_YG_MASIH_HARUS_DIBAYAR',
        'UTANG_UTANG_BAGI_HASIL',
        'UTANG_UTANG_ZAKAT',
        'UTANG_IMBALAN_PASCA_KERJA',
        'UTANG_UTANG_LAIN',
        'UTANG_JUMLAH_UTANG',
        'UTANG_JUMLAH_UTANG_PREV',
        'LIABILITAS_KONTRAK_ASURANSI_(CADANGAN_TEKNIS)_CADANGAN_PREMI_(MANFAAT_POLIS_MASA_DEPAN)',
        'LIABILITAS_KONTRAK_ASURANSI_(CADANGAN_TEKNIS)_CADANGAN_ATAS_PREMI_YANG_BELUM_MRPK_PENDAPATAN',
        'LIABILITAS_KONTRAK_ASURANSI_(CADANGAN_TEKNIS)_PENYISIHAN_KONTRIBUSI_YANG_BELUM_MENJADI_HAK',
        'LIABILITAS_KONTRAK_ASURANSI_(CADANGAN_TEKNIS)_CAD.KLAIM_(ESTIMASI_KLAIM_RETENSI_SENDIRI/EKRS)',
        'LIABILITAS_KONTRAK_ASURANSI_(CADANGAN_TEKNIS)_CADANGAN_ATAS_RISIKO_BENCANA_(CATASTROPHIC)',
        'LIABILITAS_KONTRAK_ASURANSI_(CADANGAN_TEKNIS)_JUMLAH_CADANGAN_TEKNIS',
        'LIABILITAS_KONTRAK_ASURANSI_(CADANGAN_TEKNIS)_JUMLAH_CADANGAN_TEKNIS_PREV',
        'JUMLAH_KEWAJIBAN',
        'JUMLAH_KEWAJIBAN_PREV',
        'AKUMULASI_CADANGAN_DANA_TABARRRU',
        'PINJAMAN_SUBORDINASI',
        'HAK_PEMEGANG_SAHAM_MINO-_RITAS_ATAS_AQUITAS_ANAK_PERUSAHAAN',
        'LAIN-LAIN',
        'MODALSENDIRI_MODAL_DISETOR_PREV',
        'MODALSENDIRI_MODAL_DISETOR',
        'MODALSENDIRI_TAMBAHAN_MODAL_DISETOR',
        'MODALSENDIRI_AGIO_SAHAM',
        'MODALSENDIRI_CADANGAN',
        'MODALSENDIRI_KENAIKAN_(PENURUNAN)_SURAT_BERHARGA',
        'MODALSENDIRI_SELISIH_PENILAIAN_AKTIVA_TETAP',
        'MODALSENDIRI_SELISIH_TRANSAKSI_ENTITAS_SEPENGEN-_DALI',
        'MODALSENDIRI_SELISIH_TRANSAKSI_PERUBAHAN_EKUITAS_PRSH._ANAK',
        'MODALSENDIRI_LABA_DITAHAN',
        'MODALSENDIRI_DEVIDEN',
        'MODALSENDIRI_PENDAPATAN_KOMPREHENSIF_LAIN_SETELAH_PAJAK',
        'MODALSENDIRI_KOMPONEN_EKUITAS_LAINNYA',
        'MODALSENDIRI_SALDO_LABA',
        'MODAL_SENDIRI_PREV',
        'MODAL_SENDIRI',
        'MODAL_SENDIRI_RATA-RATA_MODAL_SENDIRI',
        'JUMLAH_KEWAJIBAN_DAN_MODAL_SENDIRI',
        'PENDAPATAN_PREMI_BRUTO_PREMI_PENUTUPAN_LANGSUNG',
        'PENDAPATAN_PREMI_BRUTO_PREMI_PENUTUPAN_TIDAK_LANGSUNG',
        'PENDAPATAN_PREMI_BRUTO_KOMISI_DIBAYAR',
        'PENDAPATAN_JUMLAH_PREMI_BRUTO',
        'PENDAPATAN_JUMLAH_PREMI_BRUTO_PREV',
        'PENDAPATAN_PERUBAHAN_PENDAPATAN_PREMI_BRUTO_(%)',
        'PENDAPATAN_PREMI_REASURANSI_PREMI_REASURANSI_DIBAYAR',
        'PENDAPATAN_PREMI_REASURANSI_KOMISI_REASURANSI_DITERIMA',
        'PENDAPATAN_JUMLAH_PREMI_REASURANSI',
        'PENDAPATAN_PREMI_NETO',
        'PENDAPATAN_PENURUNAN_(KENAIKAN)_CAPYBMP*_PENURUNAN_(KENAIKAN)_CADANGAN_PEMI',
        'PENDAPATAN_PENURUNAN_(KENAIKAN)_BERJALAN_CAPYBMP',
        'PENDAPATAN_PENURUNAN_(KENAIKAN)_CAD._ATAS_RISIKO_BENCANA',
        'PENDAPATAN_PENURUNAN_(KENAIKAN)_CAPYBMP',
        'CADANGAN_PREMI',
        'PREMI_NETO_RETENSI_SENDIRI',
        'PENDAPATAN_PREMI_NETO_PREV',
        'PENDAPATAN_PREMI_NETO_CURRENT',
        'PENDAPATAN_PREMI_NETO_P_(%)',
        'CADANGAN_TEKNIS_+_MODAL_SENDIRI',
        'PENDAPATAN_UNDERWRITING_LAIN_NETO',
        'PENDAPATAN_UNDERWRITING',
        'PENDAPATAN_UNDERWRITING_PREV',
        'BEBAN_KLAIM_KLAIM_BRUTO',
        'BEBAN_KLAIM_KLAIM_REASURANSI',
        'BEBAN_KLAIM_BEBAN_KLAIM_KENAIKAN_(PENURUNAN)_EKRS/_CAD._KLAIM',
        'BEBAN_KLAIM_EKRS_TAHUN_BERJALAN',
        'BEBAN_KLAIM_EKRS_TAHUN_LALU',
        'BEBAN_KLAIM_JUMLAH_BEBAN_KLAIM_NETO',
        'BEBAN_KLAIM_JUMLAH_BEBAN_KLAIM_NETO_PREV',
        'BEBAN_UNDERWRITING_LAIN_NETTO',
        'BEBAN_UNDERWRITING',
        'BEBAN_UNDERWRITING_PREV',
        'HASIL_UNDERWRITING_PREV',
        'HASIL_UNDERWRITING',
        'HASIL_UNDERWRITING_P_(%)',
        'HASIL_INVESTASI_PREV',
        'HASIL_INVESTASI',
        'HASIL_INVESTASI_P_(%)',
        'BAGI_HASIL_PREV',
        'BAGI_HASIL',
        'BAGI_HASIL_P_(%)',
        'TOTAL_PREV',
        'TOTAL',
        'TOTAL_P_(%)',
        'BEBAN_USAHA_BEBAN_PEMASARAN',
        'BEBAN_USAHA_BEBAN_PEGAWAI_DAN_PENGURUS',
        'BEBAN_USAHA_BEBAN_UMUM_&_ADMINISTRASI_BEBAN_PENDIDIKAN_&_PELATIHAN',
        'BEBAN_USAHA_BEBAN_UMUM_&_ADMINSITRASI_LAINNYA',
        'BEBAN_USAHA_TOTAL_BEBAN_UMUM_&_ADMINSITRASI',
        'BEBAN_USAHA_BIAYA_TERKAIT_ESTIMASI_KECELAKAAN_DIRI',
        'BEBAN_USAHA_JUMLAH_BEBAN_USAHA',
        'LABA_(RUGI)_USAHA_ASURANSI',
        'HASIL_(BEBAN)_LAIN',
        'LABA_(RUGI)_SEBELUM_ZAKAT',
        'LABA_(RUGI)_SEBELUM_ZAKAT_PREV',
        'ZAKAT',
        'LABA_(RUGI)_SETELAH_ZAKAT',
        'LABA_(RUGI)_SETELAH_ZAKAT_PREV',
        'BEBAN_PAJAK_KINI',
        'BEBAN_PAJAK_TANGGUHAN',
        'JUMLAH_PAJAK_PENGHASILAN',
        'LABA_(RUGI)_SETELAH_PAJAK',
        'LABA_(RUGI)_SETELAH_PAJAK_PREV',
        'TOTAL_LABA_(RUGI)_KOMPREHENSIF_TAHUN_BERJALAN',
        'TOTAL_LABA_(RUGI)_KOMPREHENSIF_TAHUN_BERJALAN_PREV',
        'CADANGAN_TEKNIS_NETO_(KHUSUS_UNTUK_FORMULA)',
        'TINGKAT_SOLVABILITAS_(A)_ASET_YANG_DIPERKENANKAN',
        'TINGKAT_SOLVABILITAS_(A)_KEWAJIBAN',
        'TINGKAT_SOLVABILITAS_(A)_JUMLAH_TINGKAT_SOLVABILITAS',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_KREDIT_RISIKO_KEGAGALAN_DEBITUR',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_KREDIT_RISIKO_KEGAGALAN_REASURADUR',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_TOTAL_RISIKO_KREDIT',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_KREDIT_LIKUIDITAS',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_PASAR_RISIKO_PERUBAHAN_HARGA_PASAR',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_PASAR_RISIKO_PERUBAHAN_NILAI_TUKAR_MATA_UANG_ASING',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_PASAR_RISIKO_PERUBAHAN_TINGKAT_BUNGA',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_TOTAL_RISIKO_PASAR',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_ASURANSI',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_OPERASIONAL',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_JUMLAH_MMBR_(MODAL_MINIMUM_BERBASIS_RISIKO)',
        'KELEBIHAN_(KEKURANGAN)_BTS',
        'RASIO_PENCAPAIAN_(%)_RBC',
        'DANA_JAMINAN_(RP_JUTA)',
        'RASIO_LIKUIDITAS_(%)',
        'RASIO_KECUKUPAN_INVESTASI_(%)',
        'PREMI_RETENSI_SENDIRI/_MODAL_SENDIRI_(%)',
        'RASIO_PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_(%)',
        'RASIO_PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_(VERSI_INFOBANK)_(%)',
        'RASIO_BEBAN_(KLAIM,_USAHA_DAN_KOMISI)_TERHADAP_PEN-_DAPATAN_PREMI_NETO_(%)',
        'RASIO_PENCAPAIAN_SOLVABILITAS_MINIMUM_(RBC)_(%)_POSISI',
        'RASIO_PENCAPAIAN_SOLVABILITAS_MINIMUM_(RBC)_HASIL',
        'RASIO_PENCAPAIAN_SOLVABILITAS_MINIMUM_(RBC)_NILAI',
        'LIKUIDITAS_(%)_POSISI',
        'LIKUIDITAS_NILAI',
        'INVESTASI_+_ASET_LANCAR/_TOTAL_ASET_(%)_POSISI',
        'INVESTASI_+_ASET_LANCAR/_TOTAL_ASET_HASIL',
        'INVESTASI_+_ASET_LANCAR/_TOTAL_ASET_NILAI',
        'RASIO_KECUKUPAN_INVESTASI_(%)_POSISI',
        'RASIO_KECUKUPAN_INVESTASI_NILAI',
        'PERTUMBUHAN_PREMI_BRUTO_(%)_POSISI',
        'PERTUMBUHAN_PREMI_BRUTO_HASIL',
        'PERTUMBUHAN_PREMI_BRUTO_NILAI',
        'PERTUMBUHAN_MODAL_SENDIRI_(%)_POSISI',
        'PERTUMBUHAN_MODAL_SENDIRI_NILAI',
        'PREMI_RETENSI_SENDIRI/_MODAL_SENDIRI_(%)_POSISI',
        'PREMI_RETENSI_SENDIRI/_MODAL_SENDIRI_NILAI',
        'BEBAN_(KLAIM_+_USAHA_+_KOMISI)/_PENDAPATAN_PREMI_NETO_(%)_POSISI',
        'BEBAN_(KLAIM_+_USAHA_+_KOMISI)/_PENDAPATAN_PREMI_NETO_NILAI',
        'HASIL_UNDERWRITING/_PREMI_NETO_(%)_POSISI',
        'HASIL_UNDERWRITING/_PREMI_NETO_NILAI',
        'PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_(%)_POSISI',
        'PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_NILAI',
        'LABA_(RUGI)_SEBELUM_PAJAK/_RATA-RATA_MODAL_SENDIRI_(%)_POSISI',
        'LABA_(RUGI)_SEBELUM_PAJAK/_RATA-RATA_MODAL_SENDIRI_NILAI',
        'LABA_(RUGI)_KOMPREHENSIF/_RATA-RATA_MODAL_SENDIRI_(%)_POSISI',
        'LABA_(RUGI)_KOMPREHENSIF/_RATA-RATA_MODAL_SENDIRI_NILAI',
        'NILAI_TOTAL',
        'PREDIKAT',
        'PREDIKAT_PREV'
        #'PRICE_INDEX'
    ]
    #print(df.iloc[:,0])
    df.columns = columns
    report_date=get_report_date(year,'Desember')
    df['PRICE_INDEX']=1000000
    df.insert(0,'REPORT_DATE',report_date)

    df = df[df['NAMA_PERUSAHAAN'].notna()]
    df = df[~df['NAMA_PERUSAHAAN'].isin(['PERUSAHAAN ASURANSI JIWA BERPREMI BRUTO RP1 TRILIUN S.D. <RP5 TRILIUN',
                                        'PERUSAHAAN ASURANSI JIWA BERPREMI BRUTO RP250 MILIAR S. D. <RP1 TRILIUN',
                                        'Keterangan: ',
                                        'RBC: risk based capital;',
                                        '- premi neto retensi sendiri: premi bruto dikurangi premi reasuransi dibayar;',
                                        'P: peringkat berdasarkan nilai total, bila nilai total sama,',
                                        '   peringkat mengacu pada posisi RBC.', 
                                        '   peringkat mengacu pada posisi RBC.',
                                        'Sumber: Biro Riset Infobank (birI).'])]
    print(df)
    #df=df.dropna(subset=['JUMLAH BUKAN INVESTASI'])
    #df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    #print(df)
    
    table_name='ASURANSI_UMUM'
    dump_to_db(db_name_kinerja,table_name,df,report_date)
    

def insert_col_range(df,start,to):
    # Inserting the column at the
    # beginning in the DataFrame
    for loc in range(start,to):
        df.insert(loc = loc,
            column = 'col_{}'.format(loc),
            value = np.nan)
    # show the dataframe
    return df

def insert_col(df,locs):
    # Inserting the column at the
    # beginning in the DataFrame
    for loc in locs:
        df.insert(loc = loc,
            column = 'col_{}'.format(loc),
            value = np.nan)
    # show the dataframe
    return df

def drop_col(df,locs):
    # Inserting the column at the
    # beginning in the DataFrame
    df.drop(df.columns[locs], axis=1, inplace=True)
    return df

def read_all_data_asuransi_jiwa(filename,year):    
    #doing ajdustment based on year
    if year==2022:
        df=read(filename,15,2,73)
        loc=[201]
        df=drop_col(df,loc)
    elif year==2021:
        df=read(filename,13,1,67)
        locs=[55,67,170]
        df=insert_col(df,locs)
        df=insert_col_range(df,172,201)
    elif year==2019:
        df=read(filename,14,1,76)
        locs=[66]
        df=drop_col(df,locs)
        locs=[14,41,55,67]
        df=insert_col(df,locs)
        df=insert_col_range(df,172,201)
    elif year==2018:
        df=read(filename,16,1,77)
        locs=[69]
        df=drop_col(df,locs)
        locs=[14]
        df=insert_col(df,locs)
        df=insert_col_range(df,171,201)
    elif year==2017:
        df=read(filename,15,1,77)
        locs=[69]
        df=drop_col(df,locs)
        locs=[14]
        df=insert_col(df,locs)
        df=insert_col_range(df,171,201)
    elif year==2016:
        df=read(filename,14,1,73)
        #drop column no 72
        locs=[72]
        df=drop_col(df,locs)
        locs=[13,24,107,121,134,135,137,139,149,161,162]
        df=insert_col(df,locs)
        df=insert_col_range(df,172,201)
    elif year==2015:
        df=read(filename,15,1,70)
        #drop column no 70
        #print(df.iloc[:,66:73])
        locs=[67]
        df=drop_col(df,locs)
        locs=[13,24,28,107,121,134,135,137,139,149,161,162]
        df=insert_col(df,locs)
        df=insert_col_range(df,172,201)
    else:
         df=read(filename)
    
    #print(year,df.iloc[:,139:150].head(20))
    columns=[
        #'REPORT_DATE',
        'NAMA_PERUSAHAAN',
        'INVESTASI_DEPOSITO_BERJANGKA_DAN_DEPOSITO_BERJANGKA',
        'INVESTASI_SAHAM',
        'INVESTASI_OBLIGASI_DAN_MTN',
        'INVESTASI_SURAT_BERHARGA_YANG_DITERBITKAN_OLEH_NEGARA_RI',
        'INVESTASI_SURAT_BERHARGA_YANG_DITERBITKAN_OLEH_NEGARA_SELAIN_NEGARA_RI',
        'INVESTASI_SURAT_BERHARGA_YANG_DITERBITKAN_ATAU_DIJAMIN_OLEH_BI',
        'INVESTASI_SURAT_BERHARGA_YANG_DITERBITKAN_OLEH_LEMBAGA_MULTINASIONAL',
        'INVESTASI_UNIT_PENYERTAAN_REKSADANA',
        'INVESTASI_KONTRAK_INVESTASI_KOLEKTIF_EFEK_BERAGUN_ASET_(KIK_EBA)',
        'INVESTASI_DANA_INVESTASI_REAL_ESTAT',
        'INVESTASI_PENYERTAAN_LANGSUNG',
        'INVESTASI_BANGUNAN,_DENGAN_HAK_STRATA_ATAU_TANAH_DENGAN_BANGUNAN_UNTUK_INVESTASI',
        'INVESTASI_PEMBELIAN_PIUTANG_UNTUK_PERUSAHAAN_PEMBIAYAAN_DAN/_ATAU_BANK',
        'INVESTASI_PEMBIAYAAN_MELALUI_KERJASAMA_DG_PIHAK_LAIN_(EXECUTING)',
        'INVESTASI_EMAS_MURNI',
        'INVESTASI_PINJAMAN_YANG_DIJAMIN_DENGAN_HAK_TANGGUNGAN',
        'INVESTASI_PINJAMAN_POLIS',
        'INVESTASI_INVESTASI_LAIN',
        'INVESTASI_JUMLAH_INVESTASI',
        'INVESTASI_JUMLAH_INVESTASI_PREV',
        'INVESTASI_RATA-RATA_INVESTASI',
        'BUKAN_INVESTASI_KAS_DAN_BANK',
        'BUKAN_INVESTASI_TAGIHAN/_PIUTANG_PREMI_PENUTUPAN_LANGSUNG',
        'BUKAN_INVESTASI_TAGIHAN/_PIUTANG_PREMI_REASURANSI',
        'BUKAN_INVESTASI_TAGIHAN/_PIUTANG_KLAIM_KOASURANSI',
        'BUKAN_INVESTASI_TAGIHAN/_PIUTANG_KLAIM_REASURANSI',
        'BUKAN_INVESTASI_TAGIHAN/_PIUTANG_HASIL_INVESTASI',
        'BUKAN_INVESTASI_TAGIHAN/_PIUTANG_INVESTASI',
        'BUKAN_INVESTASI_TAGIHAN/_PIUTANG_LAIN-LAIN',
        'BUKAN_INVESTASI_ASET_REASURANSI',
        'BUKAN_INVESTASI_PINJAMAN_POLIS',
        'BUKAN_INVESTASI_BANGUNAN_DENGAN_HAK_STRATA_ATAU_TANAH_DENGAN_BANGUNAN_UNTUK_DIPAKAI_SENDIRI',
        'BUKAN_INVESTASI_PERANGKAT_KERAS_KOMPUTER',
        'BUKAN_INVESTASI_BIAYA_DIBAYAR_DIMUKA',
        'BUKAN_INVESTASI_PAJAK_DIBAYAR_DIMUKA',
        'BUKAN_INVESTASI_AKTIVA_PAJAK_TANGGUHAN',
        'BUKAN_INVESTASI_AKTIVA_TIDAK_BERWUJUD',
        'BUKAN_INVESTASI_AKTIVA_TETAP_LAIN',
        'BUKAN_INVESTASI_AKTIVA_LAIN',
        'BUKAN_INVESTASI_JUMLAH_BUKAN_INVESTASI',
        'BUKAN_INVESTASI_JUMLAH_BUKAN_INVESTASI_PREV_PREV',
        'JUMLAH_KEKAYAAN',
        'JUMLAH_KEKAYAAN_PREV',
        'UTANG_HUTANG_KLAIM',
        'UTANG_TITIPAN_PREMI',
        'UTANG_HUTANG_KOASURANSI',
        'UTANG_HUTANG_REASURANSI',
        'UTANG_HUTANG_KOMISI',
        'UTANG_HUTANG_PAJAK',
        'UTANG_BIAYA_YANG_MASIH_HARUS_DIBAYAR',
        'UTANG_HUTANG_ZAKAT',
        'UTANG_KEWAJIBAN_IMBALAN_PASCA_KERJA',
        'UTANG_HUTANG_LAIN',
        'UTANG_JUMLAH_UTANG',
        'UTANG_JUMLAH_UTANG_PREV',
        'CADANGAN_TEKNIS_CADANGAN_PREMI_(KEWAJIBAN_MANFAAT_POLIS_MASA_DEPAN)',
        'CADANGAN_TEKNIS_CADANGAN_PREMI_(KEWAJIBAN_PRODUK_UNIT_LINK_PIHAK_KETIGA)',
        'CADANGAN_TEKNIS_PENYISIHAN_KONTRIBUSI',
        'CADANGAN_TEKNIS_DANA_TABARRU',
        'CADANGAN_TEKNIS_TABUNGAN_PESERTA',
        'CADANGAN_TEKNIS_PENYISIHAN_KONTRIBUSI_YANG_BELUM_MEN-_JADI_PENDAPATAN',
        'CADANGAN_TEKNIS_PENYISIHAN_KLAIM',
        'CADANGAN_TEKNIS_CADANGAN_ATAS_PREMI_YANG_BELUM_MERUPAKAN_PENDAPATAN',
        'CADANGAN_TEKNIS_CADANGAN_KLAIM_(ESTIMASI_KEWAJIBAN_KLAIM)',
        'CADANGAN_TEKNIS_CADANGAN_ATAS_RISIKO_BENCANA_(CATASTROPHIC)',
        'CADANGAN_TEKNIS_JUMLAH_CADANGAN_TEKNIS',
        'CADANGAN_TEKNIS_JUMLAH_CADANGAN_TEKNIS_PREV',
        'JUMLAH_KEWAJIBAN',
        'JUMLAH_KEWAJIBAN_PREV',
        'AKUMULASI_SURPLUS_DANA_TABARRU',
        'PINJAMAN_SUBORDINASI',
        'LIABILITIS_KEPADA_PEMEGANG_UNIT_LINK',
        'HAK_PEMEGANG_SAHAM_MINORITAS',
        'KEWAJIBAN_JANGKA_PANJANG',
        'MODAL_SENDIRI_MODAL_DISETOR_PREV',
        'MODAL_SENDIRI_MODAL_DISETOR',
        'MODAL_SENDIRI_SETORAN_MODAL_LAINNYA',
        'MODAL_SENDIRI_AGIO_SAHAM',
        'MODAL_SENDIRI_CADANGAN',
        'MODAL_SENDIRI_KENAIKAN_(PENURUNAN)_INVESTASI_YANG_BELUM_DIREALISASI',
        'MODAL_SENDIRI_KENAIKAN_(PENURUNAN)_SURAT_BERHARGA',
        'MODAL_SENDIRI_SELISIH_NILAI_TRANSAKSI_RESTRUK-_TURISASI_ENTITAS_SEPENGENDALI',
        'MODAL_SENDIRI_SELISIH_PENILAIAN_AKTIVA_TETAP',
        'MODAL_SENDIRI_SAHAM_DIPEROLEH_KEMBALI',
        'MODAL_SENDIRI_PENDAPATAN_KOMPREHENSIF_LAINNYA',
        'MODAL_SENDIRI_KOMPONEN_EKUITAS_LAINNYA',
        'MODAL_SENDIRI_SALDO_LABA',
        'MODAL_SENDIRI_JUMLAH_MODAL_SENDIRI',
        'MODAL_SENDIRI_JUMLAH_MODAL_SENDIRI_PREV',
        'MODAL_SENDIRI_RATA-RATA_MODAL_SENDIRI',
        'JUMLAH_KEWAJIBAN_DAN_MODAL_SENDIRI',
        'PENDAPATAN_PERUBAHAN_PENDAPATAN_PREMI_BRUTO_(%)',
        'PENDAPATAN_PENDAPATAN_PREMI_BRUTO_PREV',
        'PENDAPATAN_PENDAPATAN_PREMI_BRUTO',
        'PENDAPATAN_PREMI_REASURANSI',
        'PENDAPATAN_PENURUNAN_(KENAIKAN)_PYBMP',
        'PENDAPATAN_PYBMP_AWAL_TAHUN_(TH._LALU)',
        'PENDAPATAN_PYBMP_AKHIR_TAHUN_(TH._BERJALAN)',
        'PENDAPATAN_JUMLAH_PENDAPATAN_PREMI_NETO',
        'PENDAPATAN_JUMLAH_PENDAPATAN_PREMI_NETO_PREV',
        'PENDAPATAN_HASIL_INVESTASI',
        'PENDAPATAN_IMBALAN_JASA_DPLK/JASA_MANAJEMEN_LAINNYA',
        'PENDAPATAN_PENDAPATAN_LAIN',
        'JUMLAH_PENDAPATAN_',
        'JUMLAH_PENDAPATAN_PREV',
        'BEBAN_KLAIM_DAN_MANFAAT_DIBAYAR',
        'BEBAN_KLAIM_PENEBUSAN_UNIT',
        'BEBAN_KLAIM_REASURANSI',
        'BEBAN_KENAIKAN_(PENURUNAN)_KMPMD',
        'BEBAN_KMPMD_TH._BERJALAN',
        'BEBAN_KLAIM_DAN_MANFAAT_KMPMD_TH._LALU',
        'BEBAN_KENAIKAN_(PENURUNAN)_DANA_TABARRU',
        'BEBAN_DANA_TABARRU_TH._BERJALAN',
        'BEBAN_DANA_TABARRU_TH._LALU',
        'BEBAN_KENAIKAN_(PENURUNAN)_TABUNGAN_PESERTA',
        'BEBAN_TAB._PESERTA_TH._BERJALAN',
        'BEBAN_TAB._PESERTA_TH._LALU',
        'BEBAN_KENAIKAN_(PENURUNAN)_EKK',
        'BEBAN_EKK_TH._BERJALAN',
        'BEBAN_EKK_TH._LALU',
        'BEBAN_KENAIKAN_(PENURUNAN)_CADANGAN_ATAS_RISIKO_BENCANA_(CTASTROPHIC)',
        'BEBAN_JUMLAH_BEBAN_KLAIM_DAN_MANFAAT',
        'BEBAN_JUMLAH_BEBAN_KLAIM_DAN_MANFAAT_PREV',
        'BEBAN_BEBAN_KOMISI_TAHUN_PERTAMA',
        'BEBAN_BIAYA_AKUISISI_BEBAN_KOMISI_TAHUN_LANJUTAN',
        'BEBAN_BEBAN_KOMISI_OVERIDING',
        'BEBAN_BEBAN_LAINNYA',
        'BEBAN_JUMLAH_BIAYA_AKUISISI',
        'BEBAN_USAHA_PEMASARAN',
        'BEBAN_USAHA_BEBAN_PEGAWAI_DAN_PENGGUNA',
        'BEBAN_USAHA_UMUM_DAN_ADMINISTRASI_BEBAN_PENDIDIKAN_DAN_PELATIHAN',
        'BEBAN_USAHA_BEBAN_LAINNYA',
        'BEBAN_USAHA_TOTAL_BEBAN_UMUM_DAN_ADMINISTRASI',
        'BEBAN_USAHA_BEBAN_MANAJEMEN',
        'BEBAN_USAHA_BEBAN_MORTALITAS',
        'BEBAN_USAHA_HASIL_(BEBAN)_LAIN',
        'TOTAL_BEBAN_USAHA',
        'JUMLAH_BEBAN',
        'KENAIKAN_(PENURUNAN)_NILAI_ASET',
        'LABA_(RUGI)_SEBELUM_ZAKAT',
        'LABA_(RUGI)_SEBELUM_ZAKAT_PREV',
        'ZAKAT',
        'LABA_(RUGI)_SEBELUM_PAJAK',
        'LABA_(RUGI)_SEBELUM_PAJAK_PREV',
        'PAJAK_PENGHASILAN',
        'LABA_(RUGI)_SETELAH_PAJAK',
        'LABA_(RUGI)_SETELAH_PAJAK_PREV',
        'LABA_(RUGI)_KOMPREHENSIP_TAHUN_BERJALAN',
        'LABA_(RUGI)_KOMPREHENSIP_TAHUN_BERJALAN_PREV',
        'PENCAPAIAN_TINGKAT_SOLVABILITAS_TINGKAT_SOLVABILITAS_(A)_ASET_YANG_DIPERKENANKAN',
        'PENCAPAIAN_TINGKAT_SOLVABILITAS_TINGKAT_SOLVABILITAS_(A)_KEWAJIBAN',
        'PENCAPAIAN_TINGKAT_SOLVABILITAS_TINGKAT_SOLVABILITAS_(A)_JUMLAH_TINGKAT_SOLVABILITAS',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_KREDIT_RISIKO_KEGAGALAN_DEBITUR',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_KREDIT_RISIKO_KEGAGALAN_REASURADUR',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_KREDIT_TOTAL_RISIKO_KREDIT',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_LIKUIDITAS',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_PASAR_RISIKO_PERUBAHAN_HARGA_PASAR',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_PASAR_RISIKO_PERUBAHAN_NILAI_TUKAR_MATA_UANG_ASING',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_PASAR_RISIKO_PERUBAHAN_TINGKAT_BUNGA',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_PASAR_TOTAL_RISIKO_PASAR',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_ASURANSI',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RISIKO_OPERASIONAL',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_TOTAL_MMBR',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_KELEBIHAN_(KEKURANGAN)_BTS',
        'MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RASIO_PENCAPAIAN_(%)_RBC',
        'INFORMASI_LAIN_JUMLAH_DANA_JAMINAN',
        'INFORMASI_LAIN_RASIO_LIKUIDITAS_(%)',
        'INFORMASI_LAIN_RASIO_KECUKUPAN_INVESTASI_(RASIO_INVESTASI_(SAP)_TERHADAP_CAD.TEKNIS_&_UTANG_KLAIM)_(%)',
        'INFORMASI_LAIN_RASIO_PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_(%)',
        'INFORMASI_LAIN_RASIO_PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_(VERSI_INFOBANK)_(%)',
        'INFORMASI_LAIN_RASIO_BEBAN_(KLAIM,_USAHA_DAN_KOMISI)_TERHADAP_PENDAPATAN_PREMI_NETO_(%)',
        'RASIO_PENCAPAIAN_SOLVABILITAS_MINIMUM_(RBC)_(%)_POSISI',
        'RASIO_PENCAPAIAN_SOLVABILITAS_MINIMUM_(RBC)_(%)_HASIL',
        'RASIO_PENCAPAIAN_SOLVABILITAS_MINIMUM_(RBC)_(%)_NILAI',
        'LIKUIDITAS_(%)_POSISI',
        'LIKUIDITAS_NILAI',
        'ASET_YANG_DIPERKENANKAN/_ASET_TOTAL_(%)_POSISI',
        'ASET_YANG_DIPERKENANKAN/_ASET_TOTAL_(%)_HASIL',
        'ASET_YANG_DIPERKENANKAN/_ASET_TOTAL_(%)_NILAI',
        'RASIO_KECUKUPAN_INVESTASI_(%)_POSISI',
        'RASIO_KECUKUPAN_INVESTASI_(%)_NILAI',
        'PERTUMBUHAN_PREMI_BRUTO_(%)_POSISI',
        'PERTUMBUHAN_PREMI_BRUTO_(%)_HASIL',
        'PERTUMBUHAN_PREMI_BRUTO_(%)_NILAI',
        'PERTUMBUHAN__MODAL_SENDIRI_(%)_POSISI',
        'PERTUMBUHAN__MODAL_SENDIRI_(%)_HASIL',
        'PERTUMBUHAN__MODAL_SENDIRI_(%)_NILAI',
        'PREMI_BRUTO/_RATA-RATA_MODAL_SENDIRI_(%)_POSISI',
        'PREMI_BRUTO/_RATA-RATA_MODAL_SENDIRI_(%)_NILAI',
        'RASIO_PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_(%)_POSISI',
        'RASIO_PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_(%)_NILAI',
        'BEBAN_(KLAIM_+_USAHA_+_KOMISI)/_PENDAPATAN_PREMI_NETO_(%)_POSISI',
        'BEBAN_(KLAIM_+_USAHA_+_KOMISI)/_PENDAPATAN_PREMI_NETO_(%)_NILAI',
        'LABA_(RUGI)_SEBELUM_PAJAK/RATA-RATA_MODAL_SENDIRI_(%)_POSISI',
        'LABA_(RUGI)_SEBELUM_PAJAK/RATA-RATA_MODAL_SENDIRI_(%)_NILAI',
        'LABA_(RUGI)_KOMPREHENSIF/_RATA-RATA_MODAL_SENDIRI_(%)_POSISI',
        'LABA_(RUGI)_KOMPREHENSIF/_RATA-RATA_MODAL_SENDIRI_(%)_NILAI',
        'NILAI_TOTAL',
        'PREDIKAT',
        'PREDIKAT_PREV'
        #'PRICE_INDEX'
    ]
    df.columns = columns
    report_date=get_report_date(year,'Desember')
    df['PRICE_INDEX']=1000000
    df.insert(0,'REPORT_DATE',report_date)

    df = df[df['NAMA_PERUSAHAAN'].notna()]
    df = df[~df['NAMA_PERUSAHAAN'].str.strip().isin(['PERUSAHAAN ASURANSI JIWA BERPREMI BRUTO RP1 TRILIUN S.D. <RP5 TRILIUN',
                                        'PERUSAHAAN ASURANSI JIWA BERPREMI BRUTO RP250 MILIAR S. D. <RP1 TRILIUN',
                                        'PERUSAHAAN ASURANSI JIWA BEPREMI BRUTO RP250 MILIAR SAMPAI DENGAN DI BAWAH RP1 TRILIUN',
                                        'PERUSAHAAN ASURANSI JIWA BEPREMI BRUTO DI BAWAH RP250 MILIAR', 
                                        'PERUSAHAAN ASURANSI JIWA BERPREMI BRUTO <RP250 MILIAR', 
                                        'Keterangan:',
                                        'RBC: risk based capital;',
                                        '#): Badan hukum berbentuk usaha bersama (mutual) ditentukan:',
                                        '- Astra AVIVA Life berubah nama menjadi Asuransi Jiwa Astra;',
                                        '- izin usaha CIMB Sun Life  dicabut per 27 Desember 2016; ', 
                                        'a): laporan keuangan tidak lengkap;',
                                        'Sumber: Biro Riset Infobank (birI).'])]
    print(df)
    #df=df.dropna(subset=['JUMLAH BUKAN INVESTASI'])
    #df=df[~(df["NAMA PERUSAHAAN"].isin(["TOTAL","RATA-RATA"]))]
    #print(df)
    
    table_name='ASURANSI_JIWA'
    dump_to_db(db_name_kinerja,table_name,df,report_date)
        
def read_data(filename,year,sector):

    df=read(filename)
    info('{}-{}'.format(year,sector))

    
    report_date=get_report_date(year,'Desember')

    if sector=='asuransi jiwa':
        dfinvestasi=df.iloc[:,:20]
        dfinvestasi['PRICE_INDEX']=1000000
        dfinvestasi.insert(0,'REPORT_DATE',report_date)
        #INVESTASI
        #investasi(dfinvestasi,report_date)

        df_company=df.iloc[:,:1]
        df_company.columns=['PERUSAHAAN ASURANSI']
        
        dfnoninvestasi = df.iloc[:,22:41]
        dfnoninvestasi=pd.concat([df_company,dfnoninvestasi],axis=1)
        dfnoninvestasi['PRICE_INDEX']=1000000
        dfnoninvestasi.insert(0,'REPORT_DATE',report_date)
        #NONINVESTASI
        #non_investasi(dfnoninvestasi,report_date)
        #UTANG
        dutang = df.iloc[:,44:55]
        dutang=pd.concat([df_company,dutang],axis=1)
        dutang['PRICE_INDEX']=1000000
        dutang.insert(0,'REPORT_DATE',report_date)
        #print(dutang)
        utang(dutang,report_date)
        #cadangan teknis
        dcadteknis = df.iloc[:,57:68]
        dcadteknis=pd.concat([df_company,dcadteknis],axis=1)
        dcadteknis['PRICE_INDEX']=1000000
        dcadteknis.insert(0,'REPORT_DATE',report_date)
        #print(dcadteknis)
        cadangan_teknis(dcadteknis,report_date)
        #liabilitas
        dlia =df.iloc[:,70:75]
        dlia=pd.concat([df_company,dlia],axis=1)
        dlia['PRICE_INDEX']=1000000
        dlia.insert(0,'REPORT_DATE',report_date)
        #print(dlia)
        liabilitas(dlia,report_date)
        #modalsendiri
        dmodal =df.iloc[:,76:89]
        dmodal=pd.concat([df_company,dmodal],axis=1)
        dmodal['PRICE_INDEX']=1000000
        dmodal.insert(0,'REPORT_DATE',report_date)
        #print(dmodal)
        modal_sendiri(dmodal,report_date)
        #pendapatan
        dpendapatan =df.iloc[:,92:105]
        dpendapatan=pd.concat([df_company,dpendapatan],axis=1)
        print(dpendapatan.head())
        #print(dpendapatan)
        dpendapatan['PRICE_INDEX']=1000000
        dpendapatan.insert(0,'REPORT_DATE',report_date)
        #print(dpendapatan)
        pendapatan(dpendapatan,report_date)
        
        #beban
        dbeban =df.iloc[:,106:129]
        dbeban=pd.concat([df_company,dbeban],axis=1)
        print(dbeban.head())
        #print(dpendapatan)
        dbeban['PRICE_INDEX']=1000000
        dbeban.insert(0,'REPORT_DATE',report_date)
        #print(dpendapatan)
        beban(dbeban,report_date)
        
    else:
        investasi(df,False)
        
def init():
    info('init')
    for root_dir, cur_dir, files in os.walk(data_folder):
        files = [file for file in files if file !=".DS_Store"]
        
    for file in files:
        try:
            #xprint(file)
            info(file)
            print(file)
            year,sector =  year_sector(file)
            info('{}-{}'.format(year,sector))
            #if not year==2015: continue
            print(year)
            if sector=='asuransi jiwa':continue
            if sector=='asuransi jiwa':
                read_all_data_asuransi_jiwa(file,year)
            else:
                read_all_data_asuransi_umum(file,year)
            
        except FileNotFoundError as e:
            error(e)

if __name__=="__main__":
    loginit()
    init()
