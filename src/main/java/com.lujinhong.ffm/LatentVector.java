package com.lujinhong.ffm;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * AUTHOR: LUJINHONG
 * CREATED ON: 17/5/26 11:45
 * PROJECT NAME: aplus_dmp
 * DESCRIPTION: latent vector
 */
class LatentVector {

    private Map<Integer, Float[]> fieldLatentVectorMap = new HashMap<>();
    private Map<Integer, Float[]> fieldAccGradientMap = new HashMap<>();

    private static float coef = (float) (0.5 / Math.sqrt(Constant.LATENT_VECTOR_DIM));
    private static Random random = new Random();

    protected LatentVector(int field) {
        fieldLatentVectorMap.put(field, getInitLatentVector());
        fieldAccGradientMap.put(field, getInitAccGradient());
    }

    protected static Float[] getInitLatentVector() {
        Float[] latentVector = new Float[Constant.LATENT_VECTOR_DIM];
        for (int i = 0; i < Constant.LATENT_VECTOR_DIM; i++) {
            int sign = 1;//TODO: remove
            latentVector[i] = coef * random.nextFloat() * sign;
        }
        return latentVector;
    }


    protected static Float[] getInitAccGradient() {
        Float[] accGradient = new Float[Constant.LATENT_VECTOR_DIM];
        for (int i = 0; i < Constant.LATENT_VECTOR_DIM; i++) {
            accGradient[i] = 1.0f;
        }
        return accGradient;
    }

    protected Map<Integer, Float[]> getFieldLatentVectorMap() {
        return fieldLatentVectorMap;
    }

    protected Map<Integer, Float[]> getFieldAccGradientMap() {
        return fieldAccGradientMap;
    }


}
