package com.demo.email;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Email {
    private String from = "NULL";
    private String subject = "NULL";
    private String date = "NULL";
    private String message = "NULL";
    private String messageid = "NULL";

    public void setFrom(String from) {
        this.from = from;
    }
    public void setSubject(String subject) {
        this.subject = subject;
    }
    public void setDate(String date) {
        this.date = date;
    }
    public void setMessage(String message_line) {
        this.message += message_line += "\n";
    }
    public void setMessageID(String messageid) {
        this.messageid = messageid;
    }
    public String getFrom() {
        return this.from;
    }
    public String getSubject() {
        return this.subject;
    }
    public String getDate() {
        return this.date;
    }
    public String getMessage() {
        return this.message;
    }
    public String getMessageID() {
        return this.messageid;
    }
}
