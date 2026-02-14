package com.hw1;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.hw1.model.Post;
import com.hw1.persistence.SQLiteDB;

/**
 * Simple Hello World application
 */
public class App {
    public static void main(String[] args) {
        try {
            // UNCOMMENT THIS:
            SQLiteDB db = new SQLiteDB("posts.db");
            // SQLiteDBReference db = new SQLiteDBReference("posts.db"); // the non-reflection version for reference only 
            
            if (args.length > 0) {
                // Load it using reflection-based approach
                String cid = args[0];
                
                // Create a dummy Post object with only the primary key populated
                Post post = new Post();
                post.setCid(cid);
                
                // Now load it
                post = db.loadRow(post);
                System.out.println("Post text: " + post.getText());
                // post = db.getPostByCid(post.getCid()); // the non-reflection version for reference only
                
                if (post != null) {
                    System.out.println("Author handle: " + post.getAuthorHandle());
                    // This one should trigger lazy loading
                    // It will return a jpeg URL content
                    
                    Object avatarContent = post.getAuthorAvatar();
                    if (avatarContent instanceof byte[]) {
                        byte[] bytes = (byte[]) avatarContent;
                        try (FileOutputStream fos = new FileOutputStream("avatar.jpeg")) {
                            fos.write(bytes);
                            System.out.println("Saved " + bytes.length + " bytes to avatar.jpeg");
                        }
                    }

                } else {
                    System.out.println("No post found for cid: " + cid);
                }
            } else {
                // Parse CSV and save to database
                List<Post> posts = readPostsFromCSV("src/main/resources/output.csv");
                
                System.out.println("Loaded " + posts.size() + " posts.");
                
                // Drop and recreate table
                db.droptTable(Post.class);
                // db.dropPostTable();
                db.createTable(Post.class);
                // db.createPostTable();
                
                // Insert all posts into the database
                int insertedCount = 0;
                for (Post post : posts) {
                    try {
                        db.insertRow(post);
                        // db.insertPost(post);
                        insertedCount++;
                    } catch (Exception e) {
                        System.err.println("Error inserting post " + post.getCid() + ": " + e.getMessage());
                    }
                }
                System.out.println("Successfully inserted " + insertedCount + " posts into database");
            }
            
            db.close();
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Other error: " + e.getMessage());
        }
    }

    public static List<Post> readPostsFromCSV(String filePath) {
        List<Post> posts = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean isFirstLine = true;
            
            while ((line = br.readLine()) != null) {
                // Skip header
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }
                
                // Parse CSV line
                String[] fields = parseCSVLine(line);
                
                if (fields.length >= 7) {
                    String cid = fields[0];
                    String authorHandle = fields[1];
                    String authorAvatarUrl = fields[2];
                    String createdAt = fields[3];
                    String text = fields[4];
                    int replyCount = Integer.parseInt(fields[5]);
                    int likeCount = Integer.parseInt(fields[6]);
                    
                    Post post = new Post(cid, authorHandle, authorAvatarUrl.getBytes(), 
                                       createdAt, text, replyCount, likeCount);
                    posts.add(post);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.err.println("Error parsing number: " + e.getMessage());
        }
        
        return posts;
    }
    
    private static String[] parseCSVLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            // some bad handling - should likely use a library here, but here we go
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }
        
        // Add the last field
        fields.add(currentField.toString());
        
        return fields.toArray(new String[0]);
    }
}
