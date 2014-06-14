package com.demo.email;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Email {
    private String from = "";
    private String from_email_id = "";
    private String from_domain_name = "";
    private String from_domain_tld = "";
    private String from_name = "";
    private String subject = "";
    private String date = "";
    private String message = "";
    private String messageid = "";

    public void setFrom(String from) {
        this.from = from;
    }
    public void setFromEmailID(String email_id) {
        this.from_email_id = email_id;
    }
    public void setFromDomainName(String from_domain_name) {
        this.from_domain_name = from_domain_name;
    }
    public void setFromDomainTLD(String from_domain_tld) {
        this.from_domain_tld = from_domain_tld;
    }
    public void setFromName(String from_name) {
        this.from_name = from_name;
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
    public String getFromEmailID() {
        return this.from;
    }
    public String getFromDomainName() {
        return this.from;
    }
    public String getFromDomainTLD() {
        return this.from;
    }
    public String getFromName() {
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
