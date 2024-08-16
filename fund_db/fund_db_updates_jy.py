# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 14:03:48 2022

@author: Wilcoxon
"""

import logging
import time

import pandas as pd

import quant_utils.data_moudle as dm
from quant_utils import constant
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import (
    DB_CONN_JJTG_DATA,
    DB_CONN_JY,
    DB_CONN_JY_LOCAL,
    DB_CONN_JY_TEST,
)
from quant_utils.send_email import MailSender
from quant_utils.utils import yield_split_list

logging.basicConfig(
    filename=constant.LOG_FILE_PATH,
    level=logging.INFO,
    format=constant.LOG_FORMAT,
)


def get_max_jsid(table: str):
    query = f"""
    select ifnull(max(JSID), 0) as JSID from {table}
    """
    return DB_CONN_JY_LOCAL.exec_query(query)["JSID"].values[0]


def query_from_remote_upsert_into_local(
    query_sql: str, table: str, query_db_conn, upsert_db_conn=DB_CONN_JJTG_DATA
):
    """
    从远程查询，插入本地数据表

    Parameters
    ----------
    query_sql : str
        查询语句
    table : str
        需要写入的表
    query_db_conn : _type_
        查询的数据库联接

    upsert_db_conn : _type_
        upsert的数据库联接
    """
    # 查询数据
    df = query_db_conn.exec_query(query_sql)
    print(f"{table}查询结束!")

    df = df.drop_duplicates()
    if not df.empty:
        upsert_db_conn.upsert(df_to_upsert=df, table=table)
    else:
        print(f"{table}-无数据插入!")
    print("==" * 40)


def update_jy_table_into_local(
    jy_table: str, query_db_conn, upsert_db_conn=DB_CONN_JY_LOCAL
):
    """
    从聚源数据库本地到数据机
    Parameters
    ----------
    jy_table : str
        聚源原表
    local_table : str
        本地表名
    query_db_conn :
        聚源查询数据连接
    upsert_db_conn : , optional
        更新数据连接, by default DB_CONN_JY_LOCAL
    """
    max_jsid = get_max_jsid(jy_table)

    qyery = f"select JSID from {jy_table} where JSID >= {max_jsid} order by JSID"
    jsid_list = query_db_conn.exec_query(qyery)["JSID"].tolist()
    jsid_small_list = list(yield_split_list(jsid_list, 500_000))
    for j, i in enumerate(jsid_small_list, start=1):
        print(f"{j}/{len(jsid_small_list)}")
        query_sql = f"""
        select * from {jy_table} where 1=1 and jsid between {i[0]} and {i[-1]}
        """

        query_from_remote_upsert_into_local(
            query_sql=query_sql,
            table=jy_table,
            query_db_conn=query_db_conn,
            upsert_db_conn=upsert_db_conn,
        )


def update_jy_db_test(step=500000):
    table_list = [
        "MF_JYFundType",
    ]
    db_conn = DB_CONN_JY_TEST
    for table in table_list:
        max_jsid = get_max_jsid(table)
        qyery = f"select JSID from {table} where JSID >= {max_jsid} order by JSID"
        jsid_list = db_conn.exec_query(qyery)["JSID"].tolist()
        jsid_small_list = list(yield_split_list(jsid_list, step))
        for j, i in enumerate(jsid_small_list, start=1):
            print(f"{j}/{len(jsid_small_list)}")
            query = f"""
            select *
            from {table} 
            where jsid between {i[0]} and {i[-1]}
            """
            query_from_remote_upsert_into_local(
                query_sql=query,
                table=table,
                query_db_conn=db_conn,
                upsert_db_conn=DB_CONN_JY_LOCAL,
            )
        print(f"{table}完成更新写入")
        print("++" * 40)


def update_jy_db_local(step=500000):
    table_dict = {"mf_purchaseandredeemn": "fund_purchaseandredeemn"}
    db_conn = DB_CONN_JY
    for table_jy, table_local in table_dict.items():
        query = f"""
            select ifnull(max(JSID), 0) as JSID from {table_local}
        """
        max_jsid = DB_CONN_JJTG_DATA.exec_query(query)["JSID"].values[0]

        qyery = f"select JSID from {table_jy} where JSID >= {max_jsid} order by JSID"
        jsid_list = db_conn.exec_query(qyery)["JSID"].tolist()
        jsid_small_list = list(yield_split_list(jsid_list, step))
        for j, i in enumerate(jsid_small_list, start=1):
            print(f"{j}/{len(jsid_small_list)}")
            query = f"""
            SELECT
                a.ID,
                a.EndDate AS TRADE_DT,
                b.SecuCode AS TICKER_SYMBOL,
                ApplyingTypeI,
                RedeemTypeI,
                ApplyingMaxI,
                ApplyingTypeII,
                RedeemTypeII,
                ApplyingMaxII,
                ApplyingTypeIII,
                RedeemTypeIII,
                ApplyingMaxIII,
                ApplyingTypeIV,
                RedeemTypeIV,
                ApplyingMaxIV,
                ApplyingTypeV,
                RedeemTypeV,
                ApplyingMaxV,
                ApplyingTypeVI,
                RedeemTypeVI,
                ApplyingMaxVI,
                ApplyingTypeVII,
                RedeemTypeVII,
                ApplyingMaxVII,
                ApplyingTypeVIII,
                RedeemTypeVIII,
                ApplyingMaxVIII,
                a.InsertTime,
                a.UpdateTime,
                a.JSID 
            FROM
                mf_purchaseandredeemn a
                JOIN secumain b ON a.InnerCode = b.InnerCode 
            WHERE
                1 =1
                and a.jsid between {i[0]} and {i[-1]}
            """
            query_from_remote_upsert_into_local(
                query_sql=query,
                table=table_local,
                query_db_conn=db_conn,
                upsert_db_conn=DB_CONN_JJTG_DATA,
            )
        print(f"{table_local}完成更新写入")
        print("++" * 40)


def del_data(update_date: str = None):
    if update_date is None:
        update_date = LAST_TRADE_DT
    query_sql = f"""
    SELECT
        TABLENAME,
        RECID AS ID 
    FROM
        jydb_deleterec a
        JOIN `information_schema`.`TABLES` b ON a.TABLENAME = b.TABLE_NAME 
    WHERE
        1 = 1 
        and XGRQ >= '{update_date}'
        AND b.TABLE_SCHEMA = 'jy_local'
    """
    del_table_data = DB_CONN_JY_LOCAL.exec_query(query_sql)
    del_data_list = []

    for table in del_table_data["TABLENAME"].unique():
        del_temp = del_table_data.query(f"TABLENAME == '{table}'")
        id_list = del_temp["ID"].tolist()
        id_list = [str(i) for i in id_list]
        id_str = ",".join(id_list)
        jsid_sql = f"""
            select ID as DEL_ID, JSID as DEL_JSID from {table.lower()} where ID in ({id_str})
        """
        del_data = DB_CONN_JY_LOCAL.exec_query(jsid_sql)
        if not del_data.empty:
            del_data_list.append(del_data)
            del_sql = f"""
                delete from {table.lower()}  where ID in ({id_str})
            """
            DB_CONN_JY_LOCAL.exec_non_query(del_sql)
            time.sleep(1)
        else:
            print("无需删除")
    # for _, val in del_table_data.iterrows():
    #     try:
    #         jsid_sql = f"""
    #         select ID as DEL_ID, JSID as DEL_JSID from {val["TABLENAME"].lower()} where ID = {val["ID"]}
    #         """
    #         del_data = DB_CONN_JY_LOCAL.exec_query(jsid_sql)
    #         if not del_data.empty:
    #             del_data["TABLE_NAME"] = val["TABLENAME"].lower()
    #             del_data_list.append(del_data)
    #             del_sql = f"""
    #             delete from {val["TABLENAME"].lower()} where ID = {val["ID"]}
    #             """
    #             DB_CONN_JY_LOCAL.exec_non_query(del_sql)
    #             time.sleep(1)
    #         else:
    #             print("无需删除")
    #     except Exception as e:
    #         print(val["TABLENAME"])
    #         print(e)
    if del_data_list:
        del_data_result = pd.concat(del_data_list)
        DB_CONN_JY_LOCAL.upsert(del_data_result, table="del_table")


def update_jy_db():
    """
    更新聚源数据库
    """
    update_jy_table_into_local(
        jy_table="jydb_deleterec",
        query_db_conn=DB_CONN_JY,
    )

    jy_tables = [
        "secumain",
        "ct_systemconst",
        "lc_instiarchive",
        "qt_tradingdaynew",
        "mf_keystockportfolio",
        "mf_mainfinancialindex",
        "mf_mainfinancialindexq",
        "mf_stockportfoliodetail",
        "mf_balancesheetnew",
        "mf_bondportifoliostru",
        "mf_incomestatementnew",
        "mf_fundtradeinfo",
        "mf_shareschange",
        "mf_fundarchives",
        "mf_chargeratenew",
        "mf_coderelationshipnew",
        "mf_fundmanagernew",
        "mf_personalinfo",
        "mf_purchaseandredeem",
        "mf_dividend",
        "mf_netvalueperformancehis",
        "mf_netvaluecashe",
        "mf_netvalue",
        "mf_financialindex",
        "mf_fundtype",
        "mf_abnormalreturn",
        "mf_brinsonperfatrb",
        "mf_calmarratio",
        "mf_campisiperfatrb",
        "mf_fundalpha",
        "mf_fundmaxdrawd",
        "mf_fundsharperatio",
        "mf_scaleanalysis",
        "mf_scalechange",
        "mf_advisorscalerank",
        "mf_issueandlisting",
        "mf_investtargetcriterion",
        "qt_sywgindexquote",
        "lc_indexbasicinfo",
        "lc_exgindustry",
        "ct_industrytype",
        "lc_announcestru",
        "mf_annclassifi",
        "mf_nottextannouncement",
        "MF_FundNetValueRe",
        "qt_interestrateindex",
        "HK_SecuMain",
        "secumainall",
        "mf_investadvisoroutline",
        "mf_trusteeoutline",
        "mf_holderinfo",
        "qt_indexquote",
        "bond_indexbasicinfo",
        "jydb_deleterec",
        "mf_risksecurityanalysis",
        "lc_indexcomponentsweight",
        "lc_shsctradestat",
        "bond_biindustry",
        "bond_code",
        "bond_conbdissueproject",
        "bond_creditgrading",
        "mf_fundportifoliodetail",
        # "lc_dindicesforvaluation",
        "mf_fcexpanalysis",
        "mf_assetallocationnew",
        "mf_fcnumofmanager",
        "lc_stockstyle",
        "mf_portfolioarchives",
        "mf_portfoliochargerate",
        "mf_portfoliodetails",
        "mf_portfolioperform",
        "mf_personalinfochange",
        "lc_indexderivative",
        "mf_bondcreditgrading",
        "MF_AssetAllocationAll",
        "mf_bondportifoliodetail",
        "mf_perfatrbfactors",
        "mf_pricereturnhis",
        "lc_zhsctradestat",
        "mf_netvalueretranstwo",
        "mf_purchaseandredeemn",
        "qt_adjustingfactor",
        "bond_cbyieldcurve",
        "mf_jyfundtype",
        "index_tagchange",
        "index_tagtype",
        "qt_osindexquote",
        "qt_goldtrademarket",
    ]

    for table in jy_tables:
        try:
            update_jy_table_into_local(
                jy_table=table,
                query_db_conn=DB_CONN_JY,
            )
        except:
            continue

    date = dm.get_now()
    mail_sender = MailSender()
    mail_sender.message_config_content(
        from_name="进化中的ChenGPT_0.1", subject=f"【聚源本地化】更新完成{date}"
    )
    mail_sender.send_mail(receivers=["569253615@qq.com"])
    del_data()


if __name__ == "__main__":
    update_jy_db()
