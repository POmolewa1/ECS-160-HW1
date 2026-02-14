package com.hw1.persistence;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;


public class SQLiteDBHelper {

    private Connection connection;

    public SQLiteDBHelper(Connection connection) {
        this.connection = connection;
    }
    
    public String getSQLType(Class<?> javaType) {
        if (javaType == String.class) {
            return "TEXT";
        } else if (javaType == int.class || javaType == Integer.class) {
            return "INTEGER";
        } else if (javaType == byte[].class) {
            return "BLOB";
        }
        return "TEXT"; // default to TEXT
    }
    
    public String camelToSnakeCase(String camelCase) {
        return camelCase.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }

    /**
     * Fetches content from a remote URL as bytes.
     * @TODO: remove the deprecated warnings
     * 
     */
    public byte[] fetchContentFromUrl(String urlString) {
        System.out.println("Fetching content from URL: " + urlString);
        if (urlString == null || urlString.isEmpty()) {
            return null;
        }
        
        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (InputStream inputStream = connection.getInputStream();
                     ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        byteArrayOutputStream.write(buffer, 0, bytesRead);
                    }
                    return byteArrayOutputStream.toByteArray();
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to fetch content from URL: " + urlString + " - " + e.getMessage());
        }
        
        // Return null if fetching fails
        return null;
    }
    
}
