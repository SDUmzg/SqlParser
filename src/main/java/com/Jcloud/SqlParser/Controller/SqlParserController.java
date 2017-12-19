package com.Jcloud.SqlParser.Controller;

import com.Jcloud.SqlParser.Common.HiveSqlParser;
import com.Jcloud.SqlParser.Common.HiveSqlParserSimple;
import com.Jcloud.SqlParser.Common.MysqlParser;
import com.Jcloud.SqlParser.Model.SqlResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by mzg on 2017/12/15.
 */
@RestController
@RequestMapping(value = "/SqlParser")
public class SqlParserController {
    @Autowired
    public HiveSqlParserSimple hiveSqlParserSimple;
    @Autowired
    public HiveSqlParser hiveSqlParser;
    @Autowired
    public MysqlParser mysqlParser;


    /**
     * 给Hive语句卖家表增加权限、格式化,Web接口
     * @param sql   需要格式化的sql
     * @param type  数据库类型"hive"
     * @param appkey  确定ISV身份的appkey
     * @param tbType  是否是卖家表 1-是  2-否
     * @return
     */
    @RequestMapping(value = "HiveSqlParserSimple")
    public SqlResult HiveSqlParserSimple(@RequestParam(value = "sql")String sql,
                                   @RequestParam(value = "type")String type,
                                   @RequestParam(value = "appkey")String appkey,
                                   @RequestParam(value = "tbType")String tbType){
        SqlResult result =  hiveSqlParserSimple.OdpsInsertParser(sql, type, appkey, tbType);
        return result;
    }

    /**
     * 基本上实现复杂的Insert的语句解析
     * @param sql     要解释的语句
     * @param appkey   Isv的appkey
     * @return
     */
    @RequestMapping(value = "HiveSqlParser")
    public SqlResult HiveSqlParser(@RequestParam(value = "sql")String sql,
                                   @RequestParam(value = "appkey")String appkey){
        SqlResult result = hiveSqlParser.HiveInsertParser(sql,appkey);
        return result;
    }



    /**
     * Mysql语句添加限制，转换成Kylin格式的数据库等
     * @param sql      Mysql语句
     * @param TranslateTo   转化后的数据库格式 KYLIN ，Other
     * @param seller_id   ISV特定的ID
     * @return 返回结果
     */
    @RequestMapping(value = "MysqlParser")
    public SqlResult MysqlParser(@RequestParam(value = "sql")String sql,
                                 @RequestParam(value = "TranslateTo")String TranslateTo,
                                 @RequestParam(value = "seller_id")String seller_id){
        SqlResult result = mysqlParser.MysqlSelectParser(sql, TranslateTo, seller_id);
        return result;
    }
}
