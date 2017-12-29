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
//                targetSql = hiveSqlParser.preFormatComplex(targetSql);
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
//                String targetSqlAfter = SQLUtils.format(targetSql, JdbcConstants.ODPS).trim();

                int [] sqlToken = getTokenNum(sqlAfter);
                int [] sqlTokenTatget = getTokenNum(targetSql);
                boolean status = true;
                for (int j =0;j<sqlToken.length;j++){
                    if (sqlToken[j]!=sqlTokenTatget[j]){
                        status = false;
                    }
                }


                if (status){
                    System.err.println("验证相同 : "+id);
                    TxtFileWriter.method1("E:\\IdeaProjects\\SqlParser\\src\\test\\java\\com\\Jcloud\\SqlParser\\Txt\\success.txt",
                            id+"\r\n");
                }else {
                    System.err.println("验证不同 : "+id);
                    TxtFileWriter.method1("E:\\IdeaProjects\\SqlParser\\src\\test\\java\\com\\Jcloud\\SqlParser\\Txt\\fail.txt",
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
    public void testSameSql(){
        int[] a=getTokenNum(sql);
        int[] b=getTokenNum(sql);
        if (a==b){
            System.out.println("两个数组相等");
        }
        for (int i=0;i<a.length;i++){
            System.err.println(i    + "  :  "+a[i]);
        }
    }


    public int[] getTokenNum(String sql){
        /**
         * tokenNum[0]  ----    select
         * tokenNum[1]  -----   from
         * tokenNum[2]  -----   join
         * tokenNum[3]  -----   on
         * tokenNum[4]  -----    where
         * tokenNum[5]  ------   insert
         * tokenNum[6]  ------   appkey
         * tokenNum[7]  ------   partition
         */
        int [] tokenNum = {0,0,0,0,0,0,0,0};
        String [] tokenStr = {"select","from","join","on","where","insert","appkey","partition"};
        for (int i=0;i<tokenNum.length;i++){
            tokenNum[i] = appearNumber(sql,tokenStr[i]);
        }
        return tokenNum;
    }

    /**
     * 获取指定字符串出现的次数
     *
     * @param srcText 源字符串
     * @param findText 要查找的字符串
     * @return
     */
    public static int appearNumber(String srcText, String findText) {
        int count = 0;
        Pattern p = Pattern.compile("\\b"+findText+"\\b",Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(srcText);
        while (m.find()) {
            count++;
        }
        return count;
    }

    final String sql = "INSERT OVERWRITE TABLE pri_upload.test PARTITION (dt='${date_ymd}')\n" +
            "SELECT shop_id, seller_id\n" +
            "FROM (\n" +
            "\tSELECT DWS_ITM_ATTENTION_D.*\n" +
            "\tFROM sys.jddp_isv_seller\n" +
            "\tJOIN dws.DWS_ITM_ATTENTION_D\n" +
            "\tON on on onm on on on jddp_isv_seller.seller_id = DWS_ITM_ATTENTION_D.seller_id\n" +
            "\t\tAND jddp_isv_seller.appkey = 'appkey123456'\n" +
            "\t\tAND jddp_isv_seller.enable_flag = '1'\n" +
            ") DWS_ITM_ATTENTION_D\n" +
            "WHERE dt = '${date_ymd-1}'";

}
