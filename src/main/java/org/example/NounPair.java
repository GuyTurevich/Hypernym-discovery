package org.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

// import java.io.DataInput;
// import java.io.DataOutput;
// import java.io.IOException;

public class NounPair implements WritableComparable<NounPair> 
{
    private Text word1;
    private Text word2;
    private IntWritable count;
    private BooleanWritable isHypernym;

    public NounPair() {
        this.word1 = new Text();
        this.word2 = new Text();
        this.count = new IntWritable();
        this.isHypernym = new BooleanWritable();
    }

    public NounPair(Text word1, Text word2, int count) {
        this.word1 = new Text(word1);
        this.word2 = new Text(word2);
        this.count = new IntWritable(count);
        this.isHypernym = new BooleanWritable(false);
    }

    public NounPair(Text word1, Text word2, boolean isHypernym) {
        this.word1 = word1;
        this.word2 = word2;
        this.count = new IntWritable(-1);
        this.isHypernym = new BooleanWritable(isHypernym);
    }

    public Text getWord1() {
        return this.word1;
    }

    public Text getWord2() {
        return this.word2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
        count.write(dataOutput);
        isHypernym.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
        count.readFields(dataInput);
        isHypernym.readFields(dataInput);
    }

    @Override
    public String toString() {
        return this.word1.toString() + "\t" + this.word2.toString() + "\t" + this.count.toString() + "\t" + this.isHypernym;
    }

    public void setWord1(Text word1) {
        this.word1 = word1;
    }

    public void setWord2(Text word2) {
        this.word2 = word2;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }


    public BooleanWritable isHypernym() {
        return isHypernym;
    }

    @Override
    public int compareTo(NounPair o) {
        return 0;
    }
}

