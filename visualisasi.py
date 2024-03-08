import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from data_analisis import *

def label_point(df, ax):
    #a = pd.concat({'x': x, 'y': y, 'val': val}, axis=1)
    for i, point in df.iterrows():
        ax.text(point['NAMA_PERUSAHAAN'], point['RBC'], str(point['REPORT_DATE'] + '\n' + point['NAMA_PERUSAHAAN']))

def analisisInvestasi():
    columns=[
        'REPORT_DATE',
        'NAMA_PERUSAHAAN',
        'DEPOSITO',
        'SAHAM',
        'OBLIGASI_MTN',
        'SB_RI',
        'SB_NONRI',
        'SB_JAMINRI',
        'SB_JAMINMULTINASIONAL',
        'REKSADANA',
        'KIK_EBA',
        'REAL_ESTATE',
        'PENYERTAAN_LNGSG',
        'TANAH_BANGUNAN',
        'PIUTAN_PEMBIAYAAN_BANK',
        'EXECUTING',
        'EMAS',
        'PINJAMAN_HAK_TANGGUNG',
        'PINJAMAN_POLIS',
        'INVESTASI_LAIN',
        'JUMLAH_INVESTASI',
        'JUMLAH_INVESTASI_PREV',
        'RATA_RATA_INVESTASI',
        'SECTOR'
    ]
    conn = create_connection('./KINERJA.db')
    query="SELECT REPORT_DATE,NAMA_PERUSAHAAN,[INVESTASI_DEPOSITO_BERJANGKA_DAN_SERTIFIKAT_DEPOSITO],\
        [INVESTASI_SAHAM],[INVESTASI_OBLIGASI_MTN_DAN_SUKUK],[INVESTASI_SURAT_BERHARGA_YG_DITERBITKAN_ATAU_DIJAMIN_OLEH_NEGARA_RI],\
        [INVESTASI_SURAT_BERHARGA_YG_DITERBITKAN_ATAU_DIJAMIN_OLEH_SELAIN_NEGARA_RI],[INVESTASI_SURAT_BERHARGA_YG_DITERBITKAN_ATAU_DIJAMIN_OLEH_BI],\
        [INVESTASI_SURAT_BERHARGA_YG_DITERBITKAN_OLEH_LEMBAGA_MULTINASIONAL],[INVESTASI_UNIT_PENYERTAAN_REKSADANA],\
        [INVESTASI_KONTRAK_INVESTASI_KOLEKTIF_EFEK_BERAGUN_ASET_(KIK_EBA)],[INVESTASI_DANA_INVESTASI_REAL_ESTAT],[INVESTASI_PENYERTAAN_LANGSUNG],\
        [INVESTASI_BANGUNAN_DENGAN_HAK_STRATA_SATU_ATAU_TANAH_DG_BANGUNAN_UNTUK_INVESTASI],[INVESTASI_PEMBELIAN_PIUTANG_UNTUK_PERUSAHAAN_PEMBIAYAAN_DAN/ATAU_BANK],\
        [INVESTASI_PEMBIAYAAN_MELALUI_KERJASAMA_DG_PIHAK_LAIN_(EXECUTING)],[INVESTASI_EMAS_MURNI],\
        [INVESTASI_PINJAMAN_YANG_DIJAMIN_DENGAN_HAK_TANGGUNGAN_(PINJAMAN_HIPOTIK)], NULL AS INVESTASI_PINJAMAN_POLIS, [INVESTASI_INVESTASI_LAIN],\
        [INVESTASI_JUMLAH_INVESTASI],[INVESTASI_JUMLAH_INVESTASI_PREV],[INVESTASI_RATA-RATA_INVESTASI],\
        'non-life' as sector from ASURANSI_UMUM order by REPORT_DATE"
    dfG = pd.read_sql_query( query, conn)
    print(dfG)
    dfG.columns=columns

    print(dfG)

    query="SELECT REPORT_DATE,NAMA_PERUSAHAAN,[INVESTASI_DEPOSITO_BERJANGKA_DAN_DEPOSITO_BERJANGKA],\
        [INVESTASI_SAHAM],[INVESTASI_OBLIGASI_DAN_MTN],[INVESTASI_SURAT_BERHARGA_YANG_DITERBITKAN_OLEH_NEGARA_RI],\
        [INVESTASI_SURAT_BERHARGA_YANG_DITERBITKAN_OLEH_NEGARA_SELAIN_NEGARA_RI],[INVESTASI_SURAT_BERHARGA_YANG_DITERBITKAN_ATAU_DIJAMIN_OLEH_BI],\
        [INVESTASI_SURAT_BERHARGA_YANG_DITERBITKAN_OLEH_LEMBAGA_MULTINASIONAL],[INVESTASI_UNIT_PENYERTAAN_REKSADANA],\
        [INVESTASI_KONTRAK_INVESTASI_KOLEKTIF_EFEK_BERAGUN_ASET_(KIK_EBA)],[INVESTASI_DANA_INVESTASI_REAL_ESTAT],[INVESTASI_PENYERTAAN_LANGSUNG],\
        [BUKAN_INVESTASI_BANGUNAN_DENGAN_HAK_STRATA_ATAU_TANAH_DENGAN_BANGUNAN_UNTUK_DIPAKAI_SENDIRI],[INVESTASI_PEMBELIAN_PIUTANG_UNTUK_PERUSAHAAN_PEMBIAYAAN_DAN/_ATAU_BANK],\
        [INVESTASI_PEMBIAYAAN_MELALUI_KERJASAMA_DG_PIHAK_LAIN_(EXECUTING)],[INVESTASI_EMAS_MURNI],\
        [INVESTASI_PINJAMAN_YANG_DIJAMIN_DENGAN_HAK_TANGGUNGAN],[INVESTASI_PINJAMAN_POLIS],[INVESTASI_INVESTASI_LAIN],\
        [INVESTASI_JUMLAH_INVESTASI],[INVESTASI_JUMLAH_INVESTASI_PREV],[INVESTASI_RATA-RATA_INVESTASI],\
        'life' as sector from ASURANSI_JIWA order by REPORT_DATE"
    print(query)
    dfL = pd.read_sql_query( query, conn)
    dfL.columns=columns
    df = pd.concat([dfG,dfL],axis=0)
    #df=convertStrToFloat(df,columns)
    print(df)
    from ydata_profiling import ProfileReport
    profile = ProfileReport(df, title='Pandas Profiling Report', html={'style':{'full_width':False}})
    profile.to_file(output_file="REPORT.html")
    #plotPerColumnDistribution(df,30,5)
    #plotCorrelationMatrix(df,10)
    #plotCorrelationMatrix(df1, 21)
    print(profile)

def ShowAllRasio(df):
    df=cleansing(df)
    df['date'] = pd.to_datetime(df['REPORT_DATE'], errors='coerce')
    df['yr']=df['date'].dt.year
    grouped = df.groupby(['yr','sector'])
    average_df = grouped.mean()    
    print(average_df)
    fig, axes = plt.subplots(3, 2, figsize=(18, 10))

    sns.barplot(ax=axes[0, 0], data=average_df, x='yr', y='RBC',hue='sector')
    sns.barplot(ax=axes[0, 1], data=average_df, x='yr', y='RKI',hue='sector')
    sns.barplot(ax=axes[1, 0], data=average_df, x='yr', y='RLiq',hue='sector')
    sns.barplot(ax=axes[1, 1], data=average_df, x='yr', y='RPHI',hue='sector')
    sns.barplot(ax=axes[2, 0], data=average_df, x='yr', y='RBPN',hue='sector')

    fig.suptitle('Analisis Kinerja Industri Asuransi (Rasio)')
    plt.subplots_adjust(hspace=0.435,wspace=0.370)
    plt.legend(loc="upper right", bbox_to_anchor=(1, 1))
    plt.show()


def RBC(df):
    df=cleansing(df)
    df=df.query('RBC <10000')
    #ax=sns.scatterplot(data=df,x='NAMA_PERUSAHAAN',y='RBC',hue='sector')
    ax=sns.scatterplot(data=df,x='NAMA_PERUSAHAAN',y='RBC',hue='sector')
    ax.set(xticklabels=[])
    #label_point(df, ax) 
    [ax.axhline(y=i, linestyle='--') for i in [120]]
    plt.suptitle('Distribusi RBC Industri Asuransi')
    plt.show()

def cleansing(df):
    df=df.dropna()
    df["RBC"] = [float(str(i).replace(",", "")) for i in df["RBC"]]
    df["RKI"] = [float(str(i).replace(",", "")) for i in df["RKI"]]
    df["RLiq"] = [float(str(i).replace(",", "")) for i in df["RLiq"]]
    df["RPHI"] = [float(str(i).replace(",", "")) for i in df["RPHI"]]
    df["RBPN"] = [float(str(i).replace(",", "")) for i in df["RBPN"]]
    return df

def Top10RBC(df,sector):
    df =cleansing(df)
    df = df[df['sector']==sector]
    df = df[df['REPORT_DATE']=='2022-12-31']
    df=df.sort_values(by=['RBC'],
               ascending=[False])
    dfchart=df.head(10)
    sns.barplot(x='RBC', y='NAMA_PERUSAHAAN', orient='h',data=dfchart)
    if sector=='life':
        plt.suptitle('Top 10 Asuransi Jiwa RBC - Tahun 2022')
    else:
        plt.suptitle('Top 10 Asuransi Umum RBC - Tahun 2022')    
    print(df.head(10))
    plt.show()

def InsolvenRBC(df,sector):
    df =cleansing(df)
    df=df.query('RBC <120')
    df = df[df['sector']==sector]
    ax=sns.scatterplot(data=df,x='NAMA_PERUSAHAAN',y='RBC',hue='sector')
    ax.set(xticklabels=[])
    label_point(df, ax) 
    if sector=='life':
        plt.suptitle('Asuransi Jiwa yang pernah RBC < 120')
    else:
        plt.suptitle('Asuransi Umum yang pernah RBC < 120')    

    print(df.head(10))
    plt.show()

def Lose10RBC(df,sector):
    df =cleansing(df)
    
    #df = df.query('RBC <120')
    
    df = df[df['sector']==sector]
    df = df[df['REPORT_DATE']=='2022-12-31']
    df=df.sort_values(by=['RBC'],
               ascending=[True])
    dfchart=df.head(10)
    sns.barplot(x='RBC', y='NAMA_PERUSAHAAN', orient='h',data=dfchart)
    if sector=='life':
        plt.suptitle('Loser 10 Asuransi Jiwa RBC - Tahun 2022')
    else:
        plt.suptitle('Loser 10 Asuransi Umum RBC - Tahun 2022')    
    print(df.head(10))
    plt.show()


def Individual_RBC(df,names):
    kwstr = '|'.join(names)
    df  = df[df['NAMA_PERUSAHAAN'].str.contains(kwstr)]
    ax=sns.scatterplot(data=df,x='REPORT_DATE',y='RBC',hue='NAMA_PERUSAHAAN')
    plt.axhline(y=120)
    plt.show()

def RasioIndustri(df):
    df['date'] = pd.to_datetime(df['Periode'], errors='coerce')
    dfChart=df[['date','ROA','ROE','Investment Yield Ratio',
            'Loss Ratio','Expense Ratio',
            'Combined Ratio','Cession Ratio','Retention Ratio',
            'Net Income Ratio','Liquid Ratio','Investment Adequacy Ratio',
            'Premium to Claim Ratio','Premium to Claim and G/A Ratio', 'sector']]
    dfChart=dfChart.dropna()

    fig, axes = plt.subplots(3, 4, figsize=(18, 10))

    sns.lineplot(ax=axes[0, 0], data=dfChart, x='date', y='ROA',hue='sector')
    sns.lineplot(ax=axes[0, 1], data=dfChart, x='date', y='ROE',hue='sector')
    sns.lineplot(ax=axes[0, 2], data=dfChart, x='date', y='Investment Yield Ratio',hue='sector')
    sns.lineplot(ax=axes[0, 3], data=dfChart, x='date', y='Loss Ratio',hue='sector')
    sns.lineplot(ax=axes[1, 0], data=dfChart, x='date', y='Expense Ratio',hue='sector')
    sns.lineplot(ax=axes[1, 1], data=dfChart, x='date', y='Combined Ratio',hue='sector')
    sns.lineplot(ax=axes[1, 2], data=dfChart, x='date', y='Cession Ratio',hue='sector')
    sns.lineplot(ax=axes[1, 3], data=dfChart, x='date', y='Retention Ratio',hue='sector')
    sns.lineplot(ax=axes[2, 0], data=dfChart, x='date', y='Net Income Ratio',hue='sector')
    sns.lineplot(ax=axes[2, 1], data=dfChart, x='date', y='Liquid Ratio',hue='sector')
    sns.lineplot(ax=axes[2, 2], data=dfChart, x='date', y='Investment Adequacy Ratio',hue='sector')
    sns.lineplot(ax=axes[2, 3], data=dfChart, x='date', y='Premium to Claim Ratio',hue='sector')
    #sns.lineplot(ax=axes[2, 3], data=dfChart, x='date', y='Premium to Claim and G/A Ratio',hue='sector')

    axes[0,0].get_legend().remove()
    axes[0,1].get_legend().remove()
    axes[0,2].get_legend().remove()
    axes[0,3].get_legend().remove()
    axes[1,0].get_legend().remove()
    axes[1,1].get_legend().remove()
    axes[1,2].get_legend().remove()
    axes[1,3].get_legend().remove()
    axes[2,0].get_legend().remove()
    axes[2,1].get_legend().remove()
    axes[2,2].get_legend().remove()
    axes[2,3].get_legend().remove()

    fig.suptitle('Analisis Kinerja Industri Asuransi')
    plt.subplots_adjust(hspace=0.435,wspace=0.370)
    plt.legend(loc="upper right", bbox_to_anchor=(1, 1))
    plt.show()

def GrowthIndustri(df):
    # Create a visualization
    # Some example data to display
    df['date'] = pd.to_datetime(df['Periode'], errors='coerce')
    dfChart=df[['date','Growth_Total Assets','Growth_Total Investment','Growth_Investment Yield',
            'Growth_Premium Income','Growth_Total Claims and Benefits',
            'Growth_Total Operating Expenses','Growth_Total Net Premium Income','sector']]
    
    dfChart=dfChart.dropna()
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle('Analisis Kinerja Industri Asuransi')

    sns.lineplot(ax=axes[0, 0], data=dfChart, x='date', y='Growth_Total Assets',hue='sector')
    sns.lineplot(ax=axes[0, 1], data=dfChart, x='date', y='Growth_Total Investment',hue='sector')
    sns.lineplot(ax=axes[0, 2], data=dfChart, x='date', y='Growth_Total Net Premium Income',hue='sector')
    sns.lineplot(ax=axes[1, 0], data=dfChart, x='date', y='Growth_Premium Income',hue='sector')
    sns.lineplot(ax=axes[1, 1], data=dfChart, x='date', y='Growth_Total Claims and Benefits',hue='sector')
    sns.lineplot(ax=axes[1, 2], data=dfChart, x='date', y='Growth_Total Operating Expenses',hue='sector')
    #sns.lineplot(ax=axes[3, 0], data=dfChart, x='date', y='Growth_Total Net Premium Income',hue='sector')
    axes[0,0].get_legend().remove()
    axes[0,1].get_legend().remove()
    axes[0,2].get_legend().remove()
    axes[1,0].get_legend().remove()
    axes[1,1].get_legend().remove()
    axes[1,2].get_legend().remove()

    plt.subplots_adjust(hspace=0.275,wspace=0.325)
    plt.legend(loc="upper right", bbox_to_anchor=(1, 1))
    plt.show()

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn



if __name__=="__main__":
    conn = create_connection('../insurance.db')
    conn1 = create_connection('./KINERJA.db')
    df = pd.read_sql_query("select * from life_non_life order by Periode", conn)
    df1 = pd.read_sql_query("select REPORT_DATE,NAMA_PERUSAHAAN,[MODAL_MINIMUM_BERBASIS_RISIKO_(MMBR)_RASIO_PENCAPAIAN_(%)_RBC] AS RBC,[INFORMASI_LAIN_RASIO_KECUKUPAN_INVESTASI_(RASIO_INVESTASI_(SAP)_TERHADAP_CAD.TEKNIS_&_UTANG_KLAIM)_(%)] AS RKI,[INFORMASI_LAIN_RASIO_LIKUIDITAS_(%)] AS RLiq,[INFORMASI_LAIN_RASIO_PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_(%)] AS RPHI,[INFORMASI_LAIN_RASIO_BEBAN_(KLAIM,_USAHA_DAN_KOMISI)_TERHADAP_PENDAPATAN_PREMI_NETO_(%)] AS RBPN,'life' AS sector from ASURANSI_JIWA order by REPORT_DATE", conn1)
    df2 = pd.read_sql_query("select REPORT_DATE,NAMA_PERUSAHAAN,[RASIO_PENCAPAIAN_(%)_RBC] AS RBC,[RASIO_KECUKUPAN_INVESTASI_(%)] as RKI,[RASIO_LIKUIDITAS_(%)] AS RLiq,[RASIO_PERIMBANGAN_HASIL_INVESTASI_DENGAN_PENDAPATAN_PREMI_NETO_(%)] AS RPHI,[RASIO_BEBAN_(KLAIM,_USAHA_DAN_KOMISI)_TERHADAP_PEN-_DAPATAN_PREMI_NETO_(%)] as RBPN,'non-life' AS sector from ASURANSI_UMUM order by REPORT_DATE", conn1)
    df3 = pd.concat([df1,df2],axis=0)
    #GrowthIndustri(df)
    #RasioIndustri(df)
    #RBC(df3)
    #Top10RBC(df3,'life')
    #Lose10RBC(df3,'life')
    #InsolvenRBC(df3,'life')
    #Top10RBC(df3,'non-life')
    #Individual_RBC(df3,['HEKSA EKA','JIWA TUGU','PASARAYA'])
    #ShowAllRasio(df3)
    analisisInvestasi()