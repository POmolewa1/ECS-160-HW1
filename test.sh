#!/bin/sh
# Generated using Copilot + Claude Opus 4.5

# total points
total_points=0

mvn clean install

# Give one point if build is successful
if [ $? -eq 0 ]; then
    total_points=$((total_points + 1))
    echo "Build successful. Points: $total_points"
else
    echo "Build failed. Points: $total_points"
    exit 1
fi

mvn exec:java

# check that posts.db is created
if [ -f posts.db ]; then
    echo "posts.db created successfully."
else
    echo "posts.db was not created."
fi

# Run sqlite3 posts.db "SELECT * FROM Post;"
res=$(sqlite3 posts.db "SELECT * FROM Post;")

# If it contains at 11 records, give 3 points
record_count=$(echo "$res" | wc -l)
if [ "$record_count" -eq 11 ]; then
    total_points=$((total_points + 3))
    echo "Database contains 11 records. Points: $total_points"
else
    echo "Database does not contain 11 records. Points: $total_points"
fi

# Try loading
load_resulsts=$(mvn exec:java -Dexec.args="bafyreiblupl33qk542futvllite5k7vnw2tld4ftgbowzwerj5zyqsn5ne")    
if echo "$load_resulsts" | grep -q "#decentralized"; then
    total_points=$((total_points + 3))
    echo "Post loaded successfully. Points: $total_points"
else
    echo "Failed to load post. Points: $total_points"
fi 

# Check if lazy loading is working by checking that "Fetching content from URL" 
# is printed AFTER "Post text: "
fetch_index=$(echo "$load_resulsts" | grep -n "Fetching content from URL" | cut -d: -f1)
post_text_index=$(echo "$load_resulsts" | grep -n "Post text: " | cut -d: -f1) 
if [ -n "$fetch_index" ] && [ -n "$post_text_index" ] && [ "$fetch_index" -gt "$post_text_index" ]; then
    # Check if avatar.jpeg file is present and is of type jpeg
    if [ -f avatar.jpeg ]; then
        file_type=$(file --mime-type -b avatar.jpeg)
        if [ "$file_type" = "image/jpeg" ]; then
            total_points=$((total_points + 3))
            echo "Lazy loading works correctly. Points: $total_points"
        else
            echo "Avatar image is not of type jpeg. Points: $total_points"
        fi
    else
        echo "Avatar image file not found. Points: $total_points"
    fi  
else
    echo "Lazy loading does not work correctly. Points: $total_points"
fi

echo "Total Points: $total_points"
