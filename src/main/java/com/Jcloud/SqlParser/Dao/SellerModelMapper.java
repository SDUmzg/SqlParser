package com.Jcloud.SqlParser.Dao;

import com.Jcloud.SqlParser.Model.SqlModel.SellerModel;
import com.Jcloud.SqlParser.Model.SqlModel.SellerModelExample;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
@Mapper
public interface SellerModelMapper {
    long countByExample(SellerModelExample example);

    int deleteByExample(SellerModelExample example);

    int deleteByPrimaryKey(Integer id);

    int insert(SellerModel record);

    int insertSelective(SellerModel record);

    List<SellerModel> selectByExample(SellerModelExample example);

    SellerModel selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") SellerModel record, @Param("example") SellerModelExample example);

    int updateByExample(@Param("record") SellerModel record, @Param("example") SellerModelExample example);

    int updateByPrimaryKeySelective(SellerModel record);

    int updateByPrimaryKey(SellerModel record);
}