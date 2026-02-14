package com.hw1.persistence;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;

public class SQLiteDB {
    private Connection connection;
    private SQLiteDBHelper dbHelper;
    
    public SQLiteDB(String dbPath) throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        this.dbHelper = new SQLiteDBHelper(this.connection);
    }
    
    public void createTable(Class<?> clazz) throws SQLException {
        
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sql.append(clazz.getSimpleName()).append(" (");
        
        Field[] fields = clazz.getDeclaredFields();
        List<Field> persistableFields = new ArrayList<>();
        
        // Only include fields marked with @Persistable annotation
        for (Field field : fields) {
            if (field.isAnnotationPresent(Persistable.class)) {
                persistableFields.add(field);
            }
        }
        
        for (int i = 0; i < persistableFields.size(); i++) {
            Field field = persistableFields.get(i);
            String fieldName = dbHelper.camelToSnakeCase(field.getName());
            String sqlType = dbHelper.getSQLType(field.getType());
            
            sql.append(fieldName).append(" ").append(sqlType);
            
            // Check if field is marked as PrimaryKey
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                sql.append(" PRIMARY KEY");
            }
            
            if (i < persistableFields.size() - 1) {
                sql.append(", ");
            }
        }
        
        sql.append(")");
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql.toString());
        }
    }
    

    public void droptTable(Class<?> clazz) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + clazz.getSimpleName();
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }   
    
    /**
     * Insert the given object into the database using reflection.
     * Only fields annotated with @Persistable should be stored.
     * Check the handout for more details.
     */
    public void insertRow(Object obj) throws SQLException, IllegalAccessException {
        List<String> fieldNames = new ArrayList<>();
        Class<?> c = obj.getClass();
        String sql = "INSERT INTO " + c.getSimpleName() + " (";

        // build the column list loop through list and add strings one by one
        for (Field f: c.getDeclaredFields()){
            if (f.isAnnotationPresent(Persistable.class)){
                f.setAccessible(true);
                fieldNames.add(dbHelper.camelToSnakeCase(f.getName()));
                //System.out.println("found filed " + f.getName());
            }
        }

        for (int i = 0; i < fieldNames.size(); i++){
            if (i != fieldNames.size() - 1){
                sql += fieldNames.get(i);
                sql += ", ";
            }
            else{
                sql += fieldNames.get(i);
                sql += ") ";
            }
        }
        // add a "Values(" and have a running count for fields and then loop through and add ?
        String s = " VALUES (";
        sql += s;
        s = "?, ";
        for (int i = 0; i < fieldNames.size(); i++){
            if (i != fieldNames.size() - 1){
                sql += s;
            }
            else{
                s = "?)";
                sql += s;
            }
        }
        //System.out.println(sql);

        // prepare the statement PreparedStatement pstmt = connection.prepareStatement(sql);
        PreparedStatement pstmt = connection.prepareStatement(sql);

        int index = 1;
        for (Field f : c.getDeclaredFields()){
            if(f.isAnnotationPresent(Persistable.class)){
                f.setAccessible(true);
                pstmt.setObject(index, f.get(obj));
                index++;
            }
        }

        pstmt.executeUpdate();
    }

    /**
     * Load a row from the database using reflection.
     * The object passed in should have its primary key field populated.
     * This method will load the other persistable fields from the database
     * and return a proxy object that lazy-loads fields annotated with @RemoteLazyLoad.
     * Check the handout for more details. 
     * 
     */
    public <T> T loadRow(T obj) throws SQLException, IllegalAccessException,Exception {
        // Return null if no row was found
        Class<?> c = obj.getClass();
        String pKeyName = "";
        Object pKey = null;

        for (Field f: c.getDeclaredFields()){
            if(f.isAnnotationPresent(PrimaryKey.class)){
                f.setAccessible(true);
                pKeyName = f.getName();
                pKey = f.get(obj);
            }
        }
        String sql = "SELECT * FROM " + obj.getClass().getSimpleName() + " WHERE " + pKeyName + " = '" + pKey + "'";

        PreparedStatement statement = connection.prepareStatement(sql);
        //System.err.println(sql);

        ResultSet result = statement.executeQuery();

        for(Field f : c.getDeclaredFields()){
            if(f.isAnnotationPresent(RemoteLazyLoad.class)){
                if(result.next()){
                    return lazyLoadProxy(obj, result, c);
                }
                else{
                    return null;
                }
            }
        }

        if (result.next()){
            for (Field f: c.getDeclaredFields()){
                if(f.isAnnotationPresent(Persistable.class)){
                    f.setAccessible(true);
                    String fieldName = dbHelper.camelToSnakeCase(f.getName());
                    //System.err.println(fieldName);
                    f.set(obj, result.getObject(fieldName));
                }
            }
        }
        else{
            return null;
        }

        //System.err.println(sql);
        return obj;
    }
    
    private <T> T lazyLoadProxy (T object, ResultSet result, Class<?> c) throws Exception{
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(c);

        String[] heldUrl = new String[1];

        MethodHandler handler = (proxy, method, proceed, args) -> {
            String calledMethod = method.getName();

            for (Field f : c.getDeclaredFields()){
                if(f.isAnnotationPresent(RemoteLazyLoad.class)){
                    String fieldMethod = f.getName();
                    String methodName = "get" + fieldMethod.substring(0,1).toUpperCase() + fieldMethod.substring(1);
                    //System.err.println(methodName);

                    if(calledMethod.equals(methodName)){
                        f.setAccessible(true);
                        byte[] currentImage = (byte[]) f.get(proxy);

                        if(currentImage == null){
                            String url = heldUrl[0];
                            if (url != null){
                                byte[] rawImage = dbHelper.fetchContentFromUrl(url);
                                f.set(proxy, rawImage);
                                return rawImage;
                            }
                        }
                        return currentImage;
                    }
                }
            }
            return proceed.invoke(proxy,args);
        };


        Class<?> pClass = factory.createClass();
        Constructor<?> pConstruct = pClass.getDeclaredConstructor();
        T proxy = (T) pConstruct.newInstance();

        ((ProxyObject) proxy).setHandler(handler);

        for(Field f : c.getDeclaredFields()){
            if (f.isAnnotationPresent(Persistable.class)){
                f.setAccessible(true);
                String currField = f.getName();
                Object currFieldVal = result.getObject(dbHelper.camelToSnakeCase(currField));

                if (f.isAnnotationPresent(RemoteLazyLoad.class)){
                    heldUrl[0] = new String((byte[])currFieldVal, StandardCharsets.UTF_8);
                    f.set(proxy, null);
                }
                else{
                    f.set(proxy, currFieldVal);
                }
            }
        }

        return proxy;
    }

    public Connection getConnection() {
        return connection;
    }
    
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}


