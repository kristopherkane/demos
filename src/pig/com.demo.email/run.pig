REGISTER Trilug-Demos-0.1.jar;

input_data = load '/user/kane/trilug/input/archive-one/' using com.demo.email.PigEmailLoader() as
    (from_who:chararray,
     from_email_id:chararray,
     from_domain_name:chararray,
     from_domain_tld:chararray,
     from_name:chararray,
     subject:chararray,
     email_date:chararray,
     messageid:chararray,
     message:chararray);

STORE input_data INTO 'trilug.emails' USING org.apache.hcatalog.pig.HCatStorer();