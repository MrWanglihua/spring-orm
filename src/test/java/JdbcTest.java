import com.demo.entity.Member;
import com.demo.entity.Order;

import javax.persistence.Column;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

public class JdbcTest {

    public static void main(String[] args) {
        Member condition = new Member();
//        condition.setName("Tom");
        condition.setAge(18);
        List<?> result =  select(condition);
        System.out.println(Arrays.toString(result.toArray()));
    }

    private static List<?> select(Object condition)  {

        List<Object> result = new ArrayList<>();

        Class<?> entityClass = condition.getClass();

        Connection con = null;
        PreparedStatement pstm = null;
        ResultSet rs = null;

        Field[] fields = entityClass.getDeclaredFields();
        //根据类名找属性名
        Map<String,String> columnMapper = new HashMap<String,String>();
        //根据属性名找字段名
        Map<String,String> fieldMapper = new HashMap<String,String>();
        for (Field field:fields) {
            if(field.isAnnotationPresent(Column.class)){
                Column column = field.getAnnotation(Column.class);
                String columnName = column.name();
                columnMapper.put(field.getAnnotation(Column.class).name(),field.getName());
                fieldMapper.put(field.getName(),columnName);
            }else{
                columnMapper.put(field.getName(),field.getName());
                fieldMapper.put(field.getName(),field.getName());
            }
        }




        try {
//              1、加载驱动类
            Class.forName("com.mysql.jdbc.Driver");
            //2、建立连接

            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/game?characterEncoding=UTF-8&rewriteBatchedStatements=true","game","admin");


            Table annotation = entityClass.getAnnotation(Table.class);
            String name = annotation.name();

            String sql = "select * from "+name;

            StringBuffer where  = new StringBuffer(" where 1=1 ");

            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);
                Object value =field.get(condition);
                if(null != value){
                    if(String.class == field.getType()) {
                        where.append(" and " + fieldMapper.get(field.getName()) + " = '" + value + "'");
                    }else{
                        where.append(" and " + fieldMapper.get(field.getName()) + " = " + value + "");
                    }
                    //其他的，在这里就不一一列举，下半截我们手写ORM框架会完善
                }

            }


            pstm = con.prepareStatement(sql+where.toString());
            rs = pstm.executeQuery();
//          getMetaData保存所以回来的元数据
            int columnCount = rs.getMetaData().getColumnCount();
            


            while(rs.next()){

                Object instance = entityClass.newInstance();

//                实体类的属性名，对应数据库的字段名
                for (int i = 1; i <=columnCount ; i++) {
                    String columnClassName = rs.getMetaData().getColumnName(i);
                    Field field = entityClass.getDeclaredField(columnMapper.get(columnClassName));
                    field.setAccessible(true);
                    field.set(instance,rs.getObject(columnClassName));

                }


                result.add(instance);
            }


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                pstm.close();
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }


        return result;
    }

}
