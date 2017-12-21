package com.Jcloud.SqlParser.Tools;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mzg on 2017/12/21.
 */
public class TxtFileReader {
    public static List<String> getTxtByLine(String path){
        List<String> result = new ArrayList<>();

        try {
            //打开文件
            FileInputStream inputStream = new FileInputStream(path);
            //获得DataInputStream对象
            DataInputStream in = new DataInputStream(inputStream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            //ReadFile Line By Line
            int count =1;
            while ((strLine = br.readLine())!=null){
                result.add(strLine);
                System.out.println(count +"   :   " +strLine);
                count++;
            }
            //Close input Stream
            in.close();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

}
