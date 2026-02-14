package com.hw1.model;

import com.hw1.persistence.Persistable;
import com.hw1.persistence.PrimaryKey;
import com.hw1.persistence.RemoteLazyLoad;

public class Post {
    @PrimaryKey
    @Persistable
    private String cid;

    @Persistable
    private String authorHandle;

    // Lazy-loaded field must always be Object type
    @Persistable
    @RemoteLazyLoad
    private byte[] authorAvatar;

    private String createdAt;

    @Persistable
    private String text;
    @Persistable
    private int replyCount;
    @Persistable
    private int likeCount;

    public Post() {
    }

    public Post(String cid, String authorHandle, byte[] authorAvatar, String createdAt, 
                String text, int replyCount, int likeCount) {
        this.cid = cid;
        this.authorHandle = authorHandle;
        this.authorAvatar = authorAvatar;
        this.createdAt = createdAt;
        this.text = text;
        this.replyCount = replyCount;
        this.likeCount = likeCount;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getAuthorHandle() {
        return authorHandle;
    }

    public void setAuthorHandle(String authorHandle) {
        this.authorHandle = authorHandle;
    }

    public byte[] getAuthorAvatar() {
        return authorAvatar;
    }

    public void setAuthorAvatar(byte[] authorAvatar) {
        this.authorAvatar = authorAvatar;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getReplyCount() {
        return replyCount;
    }

    public void setReplyCount(int replyCount) {
        this.replyCount = replyCount;
    }

    public int getLikeCount() {
        return likeCount;
    }

    public void setLikeCount(int likeCount) {
        this.likeCount = likeCount;
    }

    @Override
    public String toString() {
        return "Post{" +
                "cid='" + cid + '\'' +
                ", authorHandle='" + authorHandle + '\'' +
                ", createdAt='" + createdAt + '\'' +
                ", text='" + text + '\'' +
                ", replyCount=" + replyCount +
                ", likeCount=" + likeCount +
                '}';
    }
}
