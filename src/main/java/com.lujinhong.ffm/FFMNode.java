package com.lujinhong.ffm;

/**
 * AUTHOR: LUJINHONG
 * CREATED ON: 17/5/25 15:43
 * PROJECT NAME: aplus_dmp
 * DESCRIPTION: format:(field:feature:value)
 */
public class FFMNode {

    // feature
    private int feature;
    // field
    private int field;
    // value
    private float value;

    @Override
    public String toString() {
        return "FFMNode [field=" + field + ", feature=" + feature + ", value=" + value + "]";
    }

    public FFMNode() {
    }

    protected FFMNode(String nodeString) {
        String[] ss = nodeString.trim().split(Constant.COLON_SEPARATOR);

        field = Integer.parseInt(ss[0]);
        feature = Integer.parseInt(ss[1]);
        value = Float.parseFloat(ss[2]);

    }

    public int getFeature() {
        return feature;
    }

    public void setFeature(int f) {
        this.feature = f;
    }

    public int getField() {
        return field;
    }

    public void setField(int j) {
        this.field = j;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float v) {
        this.value = v;
    }


}
