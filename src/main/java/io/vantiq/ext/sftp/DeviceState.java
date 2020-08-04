package io.vantiq.ext.sftp;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;

@JsonPropertyOrder(value = { "deviceId","date","year","month","day","maxTime","maxValue","mean","minTime","minValue","updateTime"})
@JsonIgnoreProperties(value = { "count", "sum" })
public class DeviceState {

    /**
     *    {
     "_id": "5e888305291de36c7994a1fe",
     "deviceId": "648135774",
     "date": "2020-04-02",
     "year": 2020,
     "month": 4,
     "day": 2,
     "maxValue": 0.46,
     "maxTime": "2020-04-02 23:30:00",
     "minValue": 0.46,
     "minTime": "2020-04-02 23:30:00",
     "updateTime": "2020-04-04T12:52:21.103Z"
     }
     * */
    String deviceId;
    String date;
    Integer year;
    Integer month;
    Integer day;
    BigDecimal maxValue;
    String maxTime;
    BigDecimal minValue;
    String minTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    Date updateTime;

    BigDecimal mean;
    BigDecimal sum;

    int count;

    public DeviceState(String deviceId, String dateTime) {
        this.deviceId = deviceId;
        this.date = dateTime.substring(0, 10);
        this.year = Integer.valueOf(dateTime.substring(0, 4));
        this.month = Integer.valueOf(dateTime.substring(5, 7));
        this.day = Integer.valueOf(dateTime.substring(8, 10));
        this.maxValue = new BigDecimal(0);
        this.minValue = new BigDecimal(100);
        this.sum = new BigDecimal(0);
        this.mean = new BigDecimal(0);
        this.count = 0;
    }
    public void updateMean(BigDecimal newValue) {
        this.sum = this.sum.add(newValue);
        this.count++;
        this.mean = this.sum.divide(new BigDecimal(this.count), 4, RoundingMode.HALF_UP);
    }

    public BigDecimal getMean() {
        return mean;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }

    public BigDecimal getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(BigDecimal maxValue) {
        this.maxValue = maxValue;
    }

    public String getMaxTime() {
        return maxTime;
    }

    public void setMaxTime(String maxTime) {
        this.maxTime = maxTime;
    }

    public BigDecimal getMinValue() {
        return minValue;
    }

    public void setMinValue(BigDecimal minValue) {
        this.minValue = minValue;
    }

    public String getMinTime() {
        return minTime;
    }

    public void setMinTime(String minTime) {
        this.minTime = minTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public BigDecimal getSum() {
        return sum;
    }

    public void setSum(BigDecimal sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
