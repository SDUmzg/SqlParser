package com.Jcloud.SqlParser.Model;

/**
 * Created by mzg on 2017/12/28.
 */
public enum SqlToken {
    FROM(0),INSERT(1),SELECT(2),WHERE(3),JOIN(4),ON(5);

    private int index;
    SqlToken(int index){
        this.index = index;
    }
    public int getIndex(){
        return index;
    }
}
