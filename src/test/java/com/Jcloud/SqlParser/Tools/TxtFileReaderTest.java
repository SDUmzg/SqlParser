package com.Jcloud.SqlParser.Tools;

import com.Jcloud.SqlParser.Model.SqlModel.SellerModel;
import com.Jcloud.SqlParser.Service.SellerService;
import com.Jcloud.SqlParser.SqlParserApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by mzg on 2017/12/21.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = SqlParserApplication.class)
public class TxtFileReaderTest {
    @Autowired
    private SellerService sellerService;
    @Test
    public void getTxtByLine() throws Exception {
        String path = "E:\\IdeaProjects\\SqlParser\\src\\main\\resources\\sql\\seller.txt" ;
        List<String> tbList = TxtFileReader.getTxtByLine(path);
        int size = tbList.size();
        SellerModel sellerModel = new SellerModel();
        for (int i=0;i<size;i++){
            String tb = tbList.get(i).trim();
            if (tb==null||tb.length()==0){
                continue;
            }
            sellerModel.setName(tb);
            sellerService.insertSeller(sellerModel);

        }
    }

}