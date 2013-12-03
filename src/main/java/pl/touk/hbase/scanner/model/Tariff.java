package pl.touk.hbase.scanner.model;

import pl.touk.hadoop.hbase.annotation.Column;
import pl.touk.hadoop.hbase.annotation.Id;
import pl.touk.hadoop.hbase.annotation.Table;
import pl.touk.hadoop.hbase.annotation.Timestamp;

/**
 * @author mcl
 */
@Table("tariff_changes_50v_2")
public class Tariff {

    // 00000002 column=cf:trff_id, timestamp=1318291200000, value=15966

    String key;
    long timestamp;
    int value;

    public String getKey() {
        return key;
    }

    @Id
    public void setKey(String key) {
        this.key = key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Timestamp
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getValue() {
        return value;
    }

    @Column("cf:trff_id")
    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("%s|%s|%s\n", key, timestamp, value);
    }
}
