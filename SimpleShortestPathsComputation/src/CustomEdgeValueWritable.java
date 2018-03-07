

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomEdgeValueWritable implements WritableComparable {
    private float value;
    private char c;

    public CustomEdgeValueWritable() {
    }

    public CustomEdgeValueWritable(float value, char c) {

        this.set(value);
        this.set(c);
    }

    public void set(float value) {
        this.value = value;
    }

    public void set(char c){ this.c = c; }

    public float get() {
        return this.value;
    }

    public char getchar() { return this.c; }

    public void readFields(DataInput in) throws IOException {
        this.value = in.readFloat();
        this.c = in.readChar();
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.value);
        out.writeChar(this.c);
    }

    public boolean equals(Object o) {
        if (!(o instanceof CustomEdgeValueWritable)) {
            return false;
        } else {
            CustomEdgeValueWritable other = (CustomEdgeValueWritable)o;
            return this.value == other.value;
        }
    }

    public int hashCode() {
        return Float.floatToIntBits(this.value);
    }

    public int compareTo(Object o) {
        float thisValue = this.value;
        float thatValue = ((CustomEdgeValueWritable)o).value;
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
        String f = Float.toString(this.value);
        String ch = Character.toString(this.c);
        return f+ch;
    }

    static {
        WritableComparator.define(CustomEdgeValueWritable.class, new CustomEdgeValueWritable.Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(CustomEdgeValueWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            float thisValue = readFloat(b1, s1);
            float thatValue = readFloat(b2, s2);
            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }
}
