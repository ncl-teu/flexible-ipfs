package org.peergos.protocol.ipns;

import org.jetbrains.annotations.*;

import java.time.*;

public class IpnsRecord implements Comparable<IpnsRecord> {

    public final byte[] raw;
    public final long sequence, ttlNanos;
    public final LocalDateTime expiry;
    public final String value;

    public  byte[] rawData;

    public IpnsRecord(byte[] raw, long sequence, long ttlNanos, LocalDateTime expiry, String value) {
        this.raw = raw;
        this.sequence = sequence;
        this.ttlNanos = ttlNanos;
        this.expiry = expiry;
        this.value = value;
        this.rawData = null;
    }
    public IpnsRecord(byte[] raw, long sequence, long ttlNanos, LocalDateTime expiry, String value, byte[] rd) {
        this.raw = raw;
        this.sequence = sequence;
        this.ttlNanos = ttlNanos;
        this.expiry = expiry;
        this.value = value;
        this.rawData = rd;
    }

    public byte[] getRawData() {
        return rawData;
    }

    public void setRawData(byte[] rawData) {
        this.rawData = rawData;
    }

    @Override
    public int compareTo(@NotNull IpnsRecord b) {
        if (sequence != b.sequence)
            return (int)(sequence - b.sequence);
        if (expiry.isBefore(b.expiry))
            return -1;
        return 0;
    }
}
