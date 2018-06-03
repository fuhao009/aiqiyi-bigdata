package com.study.sparkweb.sparkweb;

import com.study.dao.CategoryClickCountDAO;
import com.study.demo.CategoryClickCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class StatCategoryApp {
    private static Map<String, String> courses = new HashMap();

    static {
        courses.put("1", "偶像爱情");
        courses.put("2", "宫斗谋权");
        courses.put("3", "玄幻史诗");
        courses.put("4", "都市生活");
        courses.put("5", "罪案谍战");
        courses.put("6", "历险科幻");
    }

    // 调用函数
    @Autowired
    // 注解 new CategoryClickCountDAO()
            // CategoryClickCountDAO categoryClickCountDAO = new CategoryClickCountDAO();
    CategoryClickCountDAO categoryClickCountDAO;

    // 调用web方法
    @RequestMapping(value = "/first", method = RequestMethod.POST)
    public List<CategoryClickCount> query() throws IOException {
        List<CategoryClickCount> list = categoryClickCountDAO.query("2018");
        for (CategoryClickCount c : list) {
            // 根据日期最后一位的栏目id匹配StatCategoryApp中设置的ID
            String name = courses.get(c.getName().substring(8));
            if (!name.equals("null")) {
                c.setName(courses.get(c.getName().substring(8)));
            } else {
                c.setName(courses.get("其他"));
            }
        }
        return list;
    }

    @RequestMapping(value = "/echarts",method = RequestMethod.GET)
    public ModelAndView echarts() {
        System.out.println("123123");
        return new ModelAndView("categoryechart");
    }

    public static void main(String[] args) throws IOException {
        StatCategoryApp app = new StatCategoryApp();
        List<CategoryClickCount> list = app.query();
        for(CategoryClickCount c:list){
            System.out.println(c.getName()+"="+c.getValue());
        }
    }

}
