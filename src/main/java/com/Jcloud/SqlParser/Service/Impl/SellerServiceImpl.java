package com.Jcloud.SqlParser.Service.Impl;

import com.Jcloud.SqlParser.Dao.SellerModelMapper;
import com.Jcloud.SqlParser.Model.ResultModel;
import com.Jcloud.SqlParser.Model.SqlModel.SellerModel;
import com.Jcloud.SqlParser.Model.SqlModel.SellerModelExample;
import com.Jcloud.SqlParser.Service.SellerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by mzg on 2017/12/21.
 */
@Service
public class SellerServiceImpl implements SellerService {

    @Autowired
    private SellerModelMapper sellerMapper;


    @Override
    public ResultModel insertSeller(SellerModel sellerModel) {
        ResultModel result = new ResultModel();
        SellerModelExample sellerModelExample = new SellerModelExample();
        SellerModelExample.Criteria criteria = sellerModelExample.createCriteria();
        criteria.andNameEqualTo(sellerModel.getName());
        long tbCount = sellerMapper.countByExample(sellerModelExample);
        if (tbCount > 0){
            result.setStatue(false);
            result.setValue("Duplicate Table");
            return result;
        }
        int status = sellerMapper.insert(sellerModel);
        if (status == 1){
            result.setStatue(true);
            result.setValue(sellerModel.toString());
        }else {
            result.setStatue(false);
            result.setValue("Error");
        }
        return result;
    }

    @Override
    public long countByTbName(String tbName) {
        SellerModelExample sellerModelExample = new SellerModelExample();
        SellerModelExample.Criteria criteria = sellerModelExample.createCriteria();
        criteria.andNameEqualTo(tbName.trim());
        long result = sellerMapper.countByExample(sellerModelExample);
        return result;
    }
}
