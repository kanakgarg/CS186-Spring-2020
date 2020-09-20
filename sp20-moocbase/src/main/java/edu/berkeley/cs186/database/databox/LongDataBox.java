package edu.berkeley.cs186.database.databox;
import java.nio.ByteBuffer;

public class LongDataBox extends DataBox {
    private long l;

    public LongDataBox(long l) {
        this.l = l;
    }

    @Override
    public Type type() {
        return Type.longType();
    }

    @Override
    public long getLong() {
        return this.l;
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Long.BYTES).putLong(l).array();
    }

    @Override
    public String toString() {
        return Long.toString(l);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LongDataBox)) {
            return false;
        }
        LongDataBox l = (LongDataBox) o;
        return this.l == l.l;
    }

    @Override
    public int hashCode() {
        return new Long(l).hashCode();
    }

    @Override
    public int compareTo(DataBox d) {
        if (!(d instanceof LongDataBox)) {
            String err = String.format("Invalid comparison between %s and %s.",
                                       toString(), d.toString());
            throw new DataBoxException(err);
        }
        LongDataBox l = (LongDataBox) d;
        return Long.compare(this.l, l.l);
    }
}
