package CountSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountBean implements WritableComparable<CountBean> {
    private int sale;
    private int brow;

    public CountBean() {
    }

    public int getSale() {
        return sale;
    }

    public void setSale(int sale) {
        this.sale = sale;
    }

    public int getBrow() {
        return brow;
    }

    public void setBrow(int brow) {
        this.brow = brow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(sale);
        dataOutput.writeInt(brow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sale = dataInput.readInt();
        brow = dataInput.readInt();
    }

    @Override
    public String toString() {
        return sale + "," + brow;
    }

    @Override
    public int compareTo(CountBean o) {
        int compare = Integer.compare(o.sale, this.sale);
        return compare == 0 ? Integer.compare(o.brow, this.brow) : compare;
    }
}
