package com.hw1.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.hw1.model.Post;

// INSTRUCTIONS: no method in this class should be used in the final submission
public class SQLiteDBReference {
    private Connection connection;
    private SQLiteDBHelper dbHelper;
    
    public SQLiteDBReference(String dbPath) throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        this.dbHelper = new SQLiteDBHelper(this.connection);
    }
    
    // non-reflection methods
    public void insertPost(Post post) throws SQLException {
        String sql = "INSERT INTO Post (cid, author_handle, author_avatar, text, reply_count, like_count) " +
                     "VALUES (?, ?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, post.getCid());
            pstmt.setString(2, post.getAuthorHandle());
            pstmt.setBytes(3, post.getAuthorAvatar());
            // pstmt.setString(4, post.getCreatedAt());
            pstmt.setString(4, post.getText());
            pstmt.setInt(5, post.getReplyCount());
            pstmt.setInt(6, post.getLikeCount());
            
            pstmt.executeUpdate();
        }
    }

    public void createPostTable() throws SQLException {
        // simple hardcoded table creation for Post class
        // use this as a reference for the reflection-based method with 
        // the signature above
        String sql = "CREATE TABLE IF NOT EXISTS Post (" +
                     "cid TEXT PRIMARY KEY, " +
                     "author_handle TEXT, " +
                     "author_avatar TEXT, " +
                     "text TEXT, " +
                     "reply_count INTEGER, " +
                     "like_count INTEGER" +
                     ")";
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public void dropPostTable() throws SQLException {
        String sql = "DROP TABLE IF EXISTS Post";
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public Post getPostByCid(String cid) throws SQLException {
        String sql = "SELECT cid, author_handle, author_avatar, text, reply_count, like_count FROM Post WHERE cid = ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, cid);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Post post = new Post(
                        rs.getString("cid"),
                        rs.getString("author_handle"),
                        rs.getBytes("author_avatar"),
                        null,
                        rs.getString("text"),
                        rs.getInt("reply_count"),
                        rs.getInt("like_count")
                    );
                    // We will replace the authorAvatar field, which is a URL string, with the contents of that 
                    // URL
                    byte[] avatarContent = dbHelper.fetchContentFromUrl(new String(post.getAuthorAvatar()));
                    post.setAuthorAvatar(avatarContent);
                    return post;
                }
            }
        }
        
        return null;
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
