import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
def getKinerja(dbname,tablename):
    # Read sqlite query results into a pandas DataFrame
    con = sqlite3.connect(dbname)
    query="SELECT REPORT_DATE,[RASIO_PENCAPAIAN_SOLVABILITAS_MINIMUM_(RBC)_(%)_POSISI] AS RBC from {}".format(tbl_name)
    df=pd.read_sql_query(query, con)
    con.close()
    return df



if __name__=="__main__":
    db_name='KINERJA.db'
    tbl_name = 'ASURANSI_JIWA'
    df=getKinerja(db_name,tbl_name)
    df=df.dropna()
    print(df)
    sns.relplot(data=df, x="REPORT_DATE", y="RBC")
    plt.show()