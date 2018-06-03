package com.study.dao;

import com.study.Utils.HBaseUtils;
import com.study.demo.CategoryClickCount;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *  将HbASE取出的数据保存到map中
 *
 */
@Component
public class CategoryClickCountDAO {
    public List<CategoryClickCount> query(String day) throws IOException{
        // 创建列表保存数据map
        List<CategoryClickCount> list = new ArrayList<>();
        // 取出数据
        Map<String,Long> map = HBaseUtils.getInstance().query("category_clickcount",day);
        // 形成list(map,map,map)
        for(Map.Entry<String,Long> entry:map.entrySet()){
//            System.out.println(entry);
            CategoryClickCount categoryClickCount = new CategoryClickCount();
            categoryClickCount.setName(entry.getKey());
            categoryClickCount.setValue(entry.getValue());
            list.add(categoryClickCount);
        }
        return list;
    }
    public static void main(String[] args) throws IOException {
        CategoryClickCountDAO dao = new CategoryClickCountDAO();
        List<CategoryClickCount> list = dao.query("2018");
        for (CategoryClickCount c : list) {
            System.out.println(c.getValue());
        }
    }
}









