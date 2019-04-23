package com.framework;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.core.common.Page;
import javax.core.common.jdbc.BaseDao;
import javax.core.common.util.GenericsUtils;
import javax.core.common.util.StringUtils;
import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public  abstract class BaseDaoSupport <T extends Serializable, PK extends Serializable> implements BaseDao<T,PK> {

    private Logger log = Logger.getLogger(BaseDaoSupport.class);
    private JdbcTemplate jdbcTemplateWrite;
    private JdbcTemplate jdbcTemplateReadOnly;

    private DataSource dataSourceReadOnly;
    private DataSource dataSourceWrite;

    public DataSource getDataSourceReadOnly() {
        return dataSourceReadOnly;
    }

    public DataSource getDataSourceWrite() {
        return dataSourceWrite;
    }

    private EntityOperation<T> op;

    private String tableName = "";

    protected BaseDaoSupport(){
        try{
            //		Class<T> entityClass = (Class<T>)((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            Class<T> entityClass = GenericsUtils.getSuperClassGenricType(getClass(), 0);
            op = new EntityOperation<T>(entityClass,this.getPKColumn());
            this.setTableName(op.tableName);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    protected void setDataSourceWrite(DataSource dataSourceWrite) {
        this.dataSourceWrite = dataSourceWrite;
        jdbcTemplateWrite = new JdbcTemplate(dataSourceWrite);
    }

    protected void setDataSourceReadOnly(DataSource dataSourceReadOnly) {
        this.dataSourceReadOnly = dataSourceReadOnly;
        jdbcTemplateReadOnly = new JdbcTemplate(dataSourceReadOnly);
    }

    private JdbcTemplate jdbcTemplateReadOnly() {
        return this.jdbcTemplateReadOnly;
    }

    private JdbcTemplate jdbcTemplateWrite() {
        return this.jdbcTemplateWrite;
    }
    /**
     * 获取表名称
     * @return
     */
    protected String getTableName() {
        return tableName;
    }


    /**
     * 获取主键列名称 建议子类重写
     * @return
     */
    protected abstract String getPKColumn();

    /**
     * 动态切换表名
     */
    protected void setTableName(String tableName) {
        if(StringUtils.isEmpty(tableName)){
            this.tableName = op.tableName;
        }else{
            this.tableName = tableName;
        }
    }

    /**
     * 根据条件查询
     * @param queryRule 查询条件
     * @return
     * @throws Exception
     */
    @Override
    public List<T> select(QueryRule queryRule) throws Exception {
        String sql ="select " +op.allColumn + " from " +getPKColumn();
        return this.jdbcTemplateReadOnly.query(sql,this.op.rowMapper,new HashMap<String,Object>());
    }

    /**
     * 分页查询
     * @param queryRule 查询条件
     * @param pageNo 页码
     * @param pageSize 每页条数
     * @return
     * @throws Exception
     */
    @Override
    public Page<?> select(QueryRule queryRule, int pageNo, int pageSize) throws Exception {
        QueryRuleSqlBuilder builder = new QueryRuleSqlBuilder(queryRule);
        Object[] values = builder.getValues();
        String ws =removeFirstAnd(builder.getWhereSql());
        String whereSql = ("".equals(ws) ? ws : (" where " + ws));
        String countSql = "select count(1) from " + getTableName() + whereSql;
        long count = (Long) this.jdbcTemplateReadOnly().queryForMap(countSql, values).get("count(1)");
        if (count == 0) {
            return new Page<T>();
        }
        long start = (pageNo - 1) * pageSize;
        // 有数据的情况下，继续查询
        String orderSql = builder.getOrderSql();
        orderSql = (StringUtils.isEmpty(orderSql) ? " " : (" order by " + orderSql));
        String sql = "select " + op.allColumn +" from " + getTableName() + whereSql + orderSql + " limit " + start + "," + pageSize;
        List<T> list = (List<T>) this.jdbcTemplateReadOnly().query(sql, this.op.rowMapper, values);
        log.debug(sql);
        return new Page<T>(start, count, pageSize, list);
    }

    private String removeFirstAnd(String sql) {
        if(StringUtils.isEmpty(sql)){return sql;}
        return sql.trim().toLowerCase().replaceAll("^\\s*and", "") + " ";
    }
    /**
     * 根据SQL语句执行查询，参数为Map
     * @param sql 语句
     * @param pamam 为Map，key为属性名，value为属性值
     * @return 符合条件的所有对象
     */
    @Override
    public List<Map<String, Object>> selectBySql(String sql, Object... pamam) throws Exception {
        return this.jdbcTemplateReadOnly().queryForList(sql,pamam);
    }
    /**
     * 分页查询特殊SQL语句
     * @param sql 语句
     * @param param  查询条件
     * @param pageNo	页码
     * @param pageSize	每页内容
     * @return
     */
    @Override
    public Page<Map<String, Object>> selectBySqlToPage(String sql, Object[] param, int pageNo, int pageSize) throws Exception {

        String countSql = "select count(1) from "+sql+") a";
        long count = (Long) this.jdbcTemplateReadOnly().queryForMap(countSql,param).get("count(1)");
        if (count == 0) {
            return new Page<Map<String,Object>>();
        }
        long start = (pageNo - 1) * pageSize;
        // 有数据的情况下，继续查询
        sql = sql + " limit " + start + "," + pageSize;
        List<Map<String,Object>> list = (List<Map<String,Object>>) this.jdbcTemplateReadOnly().queryForList(sql, param);
        log.debug(sql);
        return new Page<Map<String,Object>>(start, count, pageSize, list);
    }

    /**
     * 删除对象.<br>
     * 例如：以下删除entity对应的记录
     * <pre>
     * 		<code>
     * service.delete(entity);
     * </code>
     * </pre>
     * @param entity entity中的ID不能为空，如果ID为空，其他条件不能为空，都为空不予执行
     * @return
     * @throws Exception
     */
    @Override
    public boolean delete(T entity) throws Exception {
        return this.doDelete(op.pkField.get(entity)) > 0;
    }

    /**
     * 删除默认实例对象，返回删除记录数
     * @param pkValue
     * @return
     */
    private int doDelete(Object pkValue){
        return this.doDelete(getTableName(), getPKColumn(), pkValue);
    }

    /**
     * 删除实例对象，返回删除记录数
     * @param tableName
     * @param pkName
     * @param pkValue
     * @return
     */
    private int doDelete(String tableName, String pkName, Object pkValue) {
        StringBuffer sb = new StringBuffer();
        sb.append("delete from ").append(tableName).append(" where ").append(pkName).append(" = ?");
        int ret = this.jdbcTemplateWrite().update(sb.toString(), pkValue);
        return ret;
    }

    @Override
    public int deleteAll(List<T> list) throws Exception {
        String pkName = op.pkField.getName();
        int count = 0 ,len = list.size(),step = 1000;
        Map<String, PropertyMapping> pm = op.mappings;
        int maxPage = (len % step == 0) ? (len / step) : (len / step + 1);
        for (int i = 1; i <= maxPage; i ++) {
            StringBuffer valstr = new StringBuffer();
            Page<T> page = pagination(list, i, step);
            Object[] values = new Object[page.getRows().size()];

            for (int j = 0; j < page.getRows().size(); j ++) {
                if(j > 0 && j < page.getRows().size()){ valstr.append(","); }
                values[j] = pm.get(pkName).getter.invoke(page.getRows().get(j));
                valstr.append("?");
            }

            String sql = "delete from " + getTableName() + " where " + pkName + " in (" + valstr.toString() + ")";
            int result = jdbcTemplateWrite().update(sql, values);
            count += result;
        }
        return 0;
    }

    /**
     * 根据当前list进行相应的分页返回
     * @param objList
     * @param pageNo
     * @param pageSize
     * @return Page
     */
    protected Page<T> pagination(List<T> objList, int pageNo, int pageSize) throws Exception {
        List<T> objectArray = new ArrayList<T>(0);
        int startIndex = (pageNo - 1) * pageSize;
        int endIndex = pageNo * pageSize;
        if(endIndex >= objList.size()){
            endIndex = objList.size();
        }
        for (int i = startIndex; i < endIndex; i++) {
            objectArray.add(objList.get(i));
        }
        return new Page<T>(startIndex, objList.size(), pageSize, objectArray);
    }


    @Override
    public PK insertAndReturnId(T entity) throws Exception {
        return (PK)this.doInsertRuturnKey(parse(entity));
    }

    @Override
    public boolean insert(T entity) throws Exception {
        return this.doInsert(parse(entity));
    }
    /**
     * 批量保存对象.<br>
     * 例如：以下代码将对象保存到数据库
     * <pre>
     * 		<code>
     * List&lt;Role&gt; list = new ArrayList&lt;Role&gt;();
     * for (int i = 1; i &lt; 8; i++) {
     * 	Role role = new Role();
     * 	role.setId(i);
     * 	role.setRolename(&quot;管理quot; + i);
     * 	role.setPrivilegesFlag(&quot;1,2,3&quot;);
     * 	list.add(role);
     * }
     * service.insertAll(list);
     * </code>
     * </pre>
     *
     * @param list 待保存的对象List
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    @Override
    public int insertAll(List<T> list) throws Exception {
        int count = 0 ,len = list.size(),step = 50000;
        Map<String, PropertyMapping> pm = op.mappings;
        int maxPage = (len % step == 0) ? (len / step) : (len / step + 1);
        for (int i = 1; i <= maxPage; i ++) {
            Page<T> page = pagination(list, i, step);
            String sql = "insert into " + getTableName() + "(" + op.allColumn + ") values ";// (" + valstr.toString() + ")";
            StringBuffer valstr = new StringBuffer();
            Object[] values = new Object[pm.size() * page.getRows().size()];
            for (int j = 0; j < page.getRows().size(); j ++) {
                if(j > 0 && j < page.getRows().size()){ valstr.append(","); }
                valstr.append("(");
                int k = 0;
                for (PropertyMapping p : pm.values()) {
                    values[(j * pm.size()) + k] = p.getter.invoke(page.getRows().get(j));
                    if(k > 0 && k < pm.size()){ valstr.append(","); }
                    valstr.append("?");
                    k ++;
                }
                valstr.append(")");
            }
            int result = jdbcTemplateWrite().update(sql + valstr.toString(), values);
            count += result;
        }

        return count;
    }

    @Override
    public boolean update(T entity) throws Exception {
        return this.doUpdate(op.pkField.get(entity), parse(entity)) > 0;
    }

    /**
     * 更新实例对象，返回删除记录数
     * @param pkValue
     * @param params
     * @return
     */
    private int doUpdate(Object pkValue, Map<String, Object> params){
        //
        String sql = this.makeDefaultSimpleUpdateSql(pkValue, params);
        params.put(this.getPKColumn(), pkValue);
        int ret = this.jdbcTemplateWrite().update(sql, params.values().toArray());
        return ret;
    }
    /**
     * 生成默认的对象UPDATE语句，简化sql拼接
     * @param pkValue
     * @param params
     * @return
     */
    private String makeDefaultSimpleUpdateSql(Object pkValue, Map<String, Object> params){
        return this.makeSimpleUpdateSql(getTableName(), getPKColumn(), pkValue, params);
    }
    /**
     * 生成简单对象UPDATE语句，简化sql拼接
     * @param tableName
     * @param pkName
     * @param pkValue
     * @param params
     * @return
     */
    private String makeSimpleUpdateSql(String tableName, String pkName, Object pkValue, Map<String, Object> params){
        if(StringUtils.isEmpty(tableName) || params == null || params.isEmpty()){
            return "";
        }

        StringBuffer sb = new StringBuffer();
        sb.append("update ").append(tableName).append(" set ");
        //添加参数
        Set<String> set = params.keySet();
        int index = 0;
        for (String key : set) {
//			 sb.append(key).append(" = :").append(key);
            sb.append(key).append(" = ?");
            if(index != set.size() - 1){
                sb.append(",");
            }
            index++;
        }
//		sb.append(" where ").append(pkName).append(" = :").append(pkName) ;
        sb.append(" where ").append(pkName).append(" = ?");
        params.put("where_" + pkName,params.get(pkName));

        return sb.toString();
    }
    /**
     * 将对象解析为Map
     * @param entity
     * @return
     */
    protected Map<String,Object> parse(T entity){
        return op.parse(entity);
    }


    /**
     * 插入
     * @param params
     * @return
     */
    private boolean doInsert(Map<String, Object> params) {
        String sql = this.makeSimpleInsertSql(this.getTableName(), params);
        int ret = this.jdbcTemplateWrite().update(sql, params.values().toArray());
        return ret > 0;
    }


    /**
     * 生成对象INSERT语句，简化sql拼接
     * @param tableName
     * @param params
     * @return
     */
    private String makeSimpleInsertSql(String tableName, Map<String, Object> params){
        if(StringUtils.isEmpty(tableName) || params == null || params.isEmpty()){
            return "";
        }
        StringBuffer sb = new StringBuffer();
        sb.append("insert into ").append(tableName);

        StringBuffer sbKey = new StringBuffer();
        StringBuffer sbValue = new StringBuffer();

        sbKey.append("(");
        sbValue.append("(");
        //添加参数
        Set<String> set = params.keySet();
        int index = 0;
        for (String key : set) {
            sbKey.append(key);
//			sbValue.append(" :").append(key);
            sbValue.append(" ?");
            if(index != set.size() - 1){
                sbKey.append(",");
                sbValue.append(",");
            }
            index++;
        }
        sbKey.append(")");
        sbValue.append(")");

        sb.append(sbKey).append("VALUES").append(sbValue);

        return sb.toString();
    }

    /**
     * 生成对象INSERT语句，简化sql拼接
     * @param tableName
     * @param params
     * @return
     */
    private String makeSimpleInsertSql(String tableName, Map<String, Object> params,List<Object> values){
        if(StringUtils.isEmpty(tableName) || params == null || params.isEmpty()){
            return "";
        }
        StringBuffer sb = new StringBuffer();
        sb.append("insert into ").append(tableName);

        StringBuffer sbKey = new StringBuffer();
        StringBuffer sbValue = new StringBuffer();

        sbKey.append("(");
        sbValue.append("(");
        //添加参数
        Set<String> set = params.keySet();
        int index = 0;
        for (String key : set) {
            sbKey.append(key);
            sbValue.append(" ?");
            if(index != set.size() - 1){
                sbKey.append(",");
                sbValue.append(",");
            }
            index++;
            values.add(params.get(key));
        }
        sbKey.append(")");
        sbValue.append(")");

        sb.append(sbKey).append("VALUES").append(sbValue);

        return sb.toString();
    }



    /**
     *
     * @param params
     * @return
     */
    private Serializable doInsertRuturnKey(Map<String,Object> params){
        final List<Object> values = new ArrayList<Object>();
        final String sql = makeSimpleInsertSql(getTableName(),params,values);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        final JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSourceWrite());
        try {

            jdbcTemplate.update(new PreparedStatementCreator() {
                public PreparedStatement createPreparedStatement(

                        Connection con) throws SQLException {
                    PreparedStatement ps = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

                    for (int i = 0; i < values.size(); i++) {
                        ps.setObject(i+1, values.get(i)==null?null:values.get(i));

                    }
                    return ps;
                }

            }, keyHolder);
        } catch (DataAccessException e) {
            log.error("error",e);
        }



        if (keyHolder == null) { return ""; }


        Map<String, Object> keys = keyHolder.getKeys();
        if (keys == null || keys.size() == 0 || keys.values().size() == 0) {
            return "";
        }
        Object key = keys.values().toArray()[0];
        if (key == null || !(key instanceof Serializable)) {
            return "";
        }
        if (key instanceof Number) {
            //Long k = (Long) key;
            Class clazz = key.getClass();
//			return clazz.cast(key);
            return (clazz == int.class || clazz == Integer.class) ? ((Number) key).intValue() : ((Number)key).longValue();


        } else if (key instanceof String) {
            return (String) key;
        } else {
            return (Serializable) key;
        }


    }

}
