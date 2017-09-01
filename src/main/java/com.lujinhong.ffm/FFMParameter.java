package com.lujinhong.ffm;

/**
 * AUTHOR: LUJINHONG
 * CREATED ON: 17/5/24 11:01
 * PROJECT NAME: aplus_dmp
 * DESCRIPTION: model parameter。
 */
public class FFMParameter {
    // per-coordinate learning rate
    private float eta = 0.01f;
    // l2-regularization
    private float lambda = 0.1f;
    // latent factor dim
    private int k = 5;
    // instance-wise normalization
    private boolean normalization = true;

    public float getEta() {
        return eta;
    }

    public void setEta(float eta) {
        this.eta = eta;
    }

    public float getLambda() {
        return lambda;
    }

    public void setLambda(float lambda) {
        this.lambda = lambda;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public boolean isNormalization() {
        return normalization;
    }

    public void setNormalization(boolean normalization) {
        this.normalization = normalization;
    }




    @Override
    public String toString() {
        return "FFMParameter [eta=" + eta + ", lambda=" + lambda + ", k=" + k + ", normalization=" + normalization
                + "]";
    }
}
