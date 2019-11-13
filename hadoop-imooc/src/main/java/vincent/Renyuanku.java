package vincent;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Renyuanku implements Writable {
    private String uid;
    private String imei;
    private String imsi;
    private String mac;
    private String msisdn;

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    @Override
    public String toString() {
        return "Renyuanku{" +
                "uid='" + uid + '\'' +
                ", imei='" + imei + '\'' +
                ", imsi='" + imsi + '\'' +
                ", mac='" + mac + '\'' +
                ", msisdn='" + msisdn + '\'' +
                '}';
    }

    public Renyuanku() {
    }

    public Renyuanku(String uid, String imei, String imsi, String mac, String msisdn) {
        this.uid = uid;
        this.imei = imei;
        this.imsi = imsi;
        this.mac = mac;
        this.msisdn = msisdn;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeUTF(imei);
        out.writeUTF(imsi);
        out.writeUTF(mac);
        out.writeUTF(msisdn);
    }

    public void readFields(DataInput in) throws IOException {
        this.uid = in.readUTF();
        this.imei = in.readUTF();
        this.imsi = in.readUTF();
        this.mac = in.readUTF();
        this.msisdn = in.readUTF();
    }
}
