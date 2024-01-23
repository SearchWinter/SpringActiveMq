package com.upchina.activemq.entity;

import java.io.Serializable;

/**
 * Created by anjunli on  2024/1/5
 **/
public class Book implements Serializable {
    private static final long serialVersionUID = -174052542503056529L;
    public String Author;
    public int page;

    public Book() {
    }

    public Book(String author, int page) {
        Author = author;
        this.page = page;
    }

    @Override
    public String toString() {
        return "Book{" +
                "Author='" + Author + '\'' +
                ", page=" + page +
                '}';
    }
}
