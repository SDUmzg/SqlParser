package com.Jcloud.SqlParser.Common;

import com.Jcloud.SqlParser.Model.SqlResult;
import com.alibaba.druid.sql.ast.SQLStatement;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by mzg on 2017/12/13.
 */
public class HiveSqlParserTest {
    @Test
    public void odpsInsertParser() throws Exception {
        HiveSqlParser hiveSqlParser = new HiveSqlParser();
        SqlResult sqlResult = hiveSqlParser.OdpsInsertParser(sql1,"hive","1","1");
        hiveSqlParser.OdpsInsertParser(sqlResult.getValue(),"hive","1","1");

    }

    @Test
    public void tempTest() throws Exception{
    }










    public static final String sql=" INSERT OVERWRITE  TABLE   pri_result.dws_itm_platform_plot_trade_d \n" +
            " SELECT \n" +
            "    * \n" +
            " FROM \n" +
            "    dws.dws_itm_platform_plot_trade_d   \n" +
            "\n" +
            " WHERE \n" +
            "    dt='${date_ymd}';\n";


    public static final String sql1=" INSERT OVERWRITE  TABLE   pri_result.dws_itm_platform_plot_trade_d\n" +
            " SELECT \n" +
            "    dws_itm_platform_plot_trade_d.the_dt,\n" +
            "    dws_itm_platform_plot_trade_d.shop_id,\n" +
            "    dws_itm_platform_plot_trade_d.seller_id\n" +
            " FROM \n" +
            "    dws.dws_itm_platform_plot_trade_d  \n" +
            " where \n" +
            "    dws_itm_platform_plot_trade_d.dt='${date_ymd}' \n" +
            " OR  \n" +
            "    dws_itm_platform_plot_trade_d.seller_id='20140030'\n" +
            " ORDER BY\n" +
            "    dws_itm_platform_plot_trade_d.shop_id;\n";

    public static final String sql2=" INSERT OVERWRITE  TABLE   pri_result.dws_itm_platform_plot_trade_d\n" +
            " SELECT \n" +
            "    abc.the_dt,\n" +
            "    abc.shop_id,\n" +
            "    abc.seller_id\n" +
            " FROM \n" +
            "    (select * from dws.dws_itm_platform_plot_trade_d join dws.a ) abc  \n" +
            " WHERE \n" +
            "    abc.dt='${date_ymd}';\n";


}