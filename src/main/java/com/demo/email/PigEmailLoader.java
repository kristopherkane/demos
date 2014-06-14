package com.demo.email;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

public class PigEmailLoader extends LoadFunc {
    private RecordReader reader;
    private TupleFactory tupleFactory;

    public PigEmailLoader() {
        tupleFactory = TupleFactory.getInstance();
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new EmailFileInputFormat();
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple tuple = null;
        try {
            boolean notDone = reader.nextKeyValue();
            if (!notDone) {
                return null;
            }
            Email email = (Email) reader.getCurrentValue();
            tuple=tupleFactory.newTuple(5);

            tuple.set(0, email.getFrom());
            tuple.set(1, email.getSubject());
            tuple.set(2, email.getDate());
            tuple.set(3, email.getMessageID());
            tuple.set(4, email.getMessage());
            DataBag dataBag =  BagFactory.getInstance().newDefaultBag();

            dataBag.add(tuple);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return tuple;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit pigSplit)
            throws IOException {
        this.reader = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        EmailFileInputFormat.setInputPaths(job, location);
    }
}
