package model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Event {
    private String ts;
    private String user_id;
    private String actor;
    private double v1;
    private double v2;
    private double v3;
    private double v4;
    private double v5;
    private double v6;
    private ZipCode location;


    public Event() {
    }

    public Event(String ts, String user_id, String actor, double v1, double v2, double v3, double v4, double v5, double v6,
                 ZipCode zipCode) {
        this.ts = ts;
        this.user_id = user_id;
        this.actor = actor;
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
        this.v4 = v4;
        this.v5 = v5;
        this.v6 = v6;
        this.location = zipCode;
    }


    public double getV1() {
        return v1;
    }


    public double getV2() {
        return v2;
    }


    public double getV3() {
        return v3;
    }


    public double getV4() {
        return v4;
    }


    public double getV5() {
        return v5;
    }


    public double getV6() {
        return v6;
    }


    public String getTs() {
        return ts;
    }


    public String getActor() {
        return actor;
    }


    public String getUser_id() {
        return user_id;
    }


    public void setActor(String actor) {
        this.actor = actor;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public void setLocation(ZipCode location) {
        this.location = location;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public void setV1(double v1) {
        this.v1 = v1;
    }

    public void setV2(double v2) {
        this.v2 = v2;
    }

    public void setV3(double v3) {
        this.v3 = v3;
    }

    public void setV4(double v4) {
        this.v4 = v4;
    }

    public void setV5(double v5) {
        this.v5 = v5;
    }

    public void setV6(double v6) {
        this.v6 = v6;
    }

    public ZipCode getLocation() {
        return location;
    }


}
