package com.Jcloud.SqlParser;

import com.Jcloud.SqlParser.Common.HiveSqlParser;
import com.Jcloud.SqlParser.Dao.HiveTestModelMapper;
import com.Jcloud.SqlParser.Model.ResultModel;
import com.Jcloud.SqlParser.Model.SqlModel.HiveTestModel;
import com.Jcloud.SqlParser.Model.SqlModel.HiveTestModelExample;
import com.Jcloud.SqlParser.Model.SqlResult;
import com.Jcloud.SqlParser.Tools.TxtFileWriter;
import com.alibaba.druid.Constants;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.util.JdbcConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SqlParserApplicationTests {

    @Autowired
    private HiveTestModelMapper hiveTestModelMapper;
    @Autowired
    private HiveSqlParser hiveSqlParser;

    @Test
    public void contextLoads() {

    }

    @Test
    public void testHive(){
        HiveTestModelExample hiveTestModelExample = new HiveTestModelExample();
        HiveTestModelExample.Criteria criteria = hiveTestModelExample.createCriteria();
        criteria.andDevSqlIsNotNull();
        criteria.andDevSqlAutoIsNotNull();
        List<HiveTestModel> hiveTestModelList = hiveTestModelMapper.selectByExample(hiveTestModelExample);
        int size = hiveTestModelList.size();
        for (int i=0;i<size;i++){
            HiveTestModel hiveTestModel =  hiveTestModelList.get(i);
            String sql = hiveTestModel.getDevSql();
            String targetSql = hiveTestModel.getDevSqlAuto();
            int id = hiveTestModel.getId();
            System.out.println("id  :  "+id);

            try{
                if (sql==null||sql.trim().length()==0){
                    continue;
                }
                if (targetSql==null || targetSql.trim().length()==0){
                    continue;
                }
                sql = hiveSqlParser.preFormat(sql);
                targetSql = hiveSqlParser.preFormat(targetSql);
                String appkey = "appkey12345678";
                if (targetSql.indexOf("appkey")!=-1){
                    String pattern = ".*?appkey.*?'(.*?)'.*?";
                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(targetSql);
                    if (m.find()){
                        String c = m.group(1);
                        appkey = c;
                    }

                }

                //本地解析一遍的SQL
                SqlResult afterParser = hiveSqlParser.HiveInsertParser(sql,appkey);
                String sqlAfterTemp = afterParser.getValue().trim();
                String sqlAfter = SQLUtils.format(sqlAfterTemp, JdbcConstants.ODPS).trim();

                //数据库中的数据格式化
                String targetSqlAfter = SQLUtils.format(targetSql, JdbcConstants.ODPS).trim();

                if (!sqlAfter.equals(targetSqlAfter)){
                    System.err.println("验证不同 : "+id);
                    TxtFileWriter.method1("E:\\IdeaProjects\\SqlParser\\src\\test\\java\\com\\Jcloud\\SqlParser\\Txt\\fail.txt",
                            id+"\r\n");
                }else {
                    System.err.println("验证通过 : "+id);
                    TxtFileWriter.method1("E:\\IdeaProjects\\SqlParser\\src\\test\\java\\com\\Jcloud\\SqlParser\\Txt\\success.txt",
                            id+"\r\n");
                }

            }catch (Exception e){
                System.err.println("ERROR " + id);
                TxtFileWriter.method1("E:\\IdeaProjects\\SqlParser\\src\\test\\java\\com\\Jcloud\\SqlParser\\Txt\\error.txt",
                        id+"\r\n");
            }

        }


//		System.err.println(hiveTestModelList.size());
    }

    @Test
    public void testSimple(){
        String sql =  "insert overwrite table pri_temp.mytmp partition(dt = '${date_ymd}')  select *    from pri_result.myresult where dt='${date_ymd}'";
        String targetSql = "from  (from sys.jddp_isv_seller join dws.dws_itm_asso_d on (jddp_isv_seller.seller_id = dws_itm_asso_d.seller_id and jddp_isv_seller.appkey = '6acbc14f3ce443ffcd86502ae9df6ac9' and jddp_isv_seller.enable_flag = '1') select dws_itm_asso_d.*) dws_itm_asso_d insert overwrite table pri_temp.mytmp partition(dt = '${date_ymd}') select sku_id,shop_id where dt = '${date_ymd}'";
        try{
            sql = hiveSqlParser.preFormat(sql);
            targetSql = hiveSqlParser.preFormat(targetSql);
            String appkey = "appkey12345678";
            if (targetSql.indexOf("appkey")!=-1){
                String pattern = ".*?appkey.*?'(.*?)'.*?";
                Pattern r = Pattern.compile(pattern);
                Matcher m = r.matcher(targetSql);
                if (m.find()){
                    String c = m.group(1);
                    appkey = c;
                }
            }
            System.out.println(appkey);

            //本地解析一遍的SQL
            SqlResult afterParser = hiveSqlParser.HiveInsertParser(sql,appkey);
            String sqlAfterTemp = afterParser.getValue().trim();
            String sqlAfter = SQLUtils.format(sqlAfterTemp, JdbcConstants.ODPS).trim();

            //数据库中的数据格式化
            String targetSqlAfter = SQLUtils.format(targetSql, JdbcConstants.ODPS).trim();

            System.out.println(sqlAfter);
            System.out.println("-----------------------------------");
            System.out.println(targetSqlAfter);


            if (!sqlAfter.equals(targetSqlAfter)){
                System.err.println("验证不同 : ");
            }else {
                System.err.println("验证通过 : ");
            }

        }catch (Exception e){
            System.err.println("ERROR ");
        }
    }


}
