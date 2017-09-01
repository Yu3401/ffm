package com.lujinhong.ffm;

import com.lujinhong.ffm.utils.HDFSUtils;
import com.lujinhong.ffm.utils.ROCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * AUTHOR: LUJINHONG
 * CREATED ON: 17/5/23 11:45
 * PROJECT NAME: aplus_dmp
 * DESCRIPTION: implementation of ffm
 */


public class FFM {
    private static final Logger LOG = LoggerFactory.getLogger(FFM.class);
    private static final int IMBALANCE_FACTOR = 1; //modify according to your sample distribution
    private static ROCUtils rocUtils = new ROCUtils();
    protected static final String TAB_SEPARATOR = "\t";

    private static Map<Integer, LatentVector> W = new HashMap<>();
    private static Map<Integer, Float> W1 = new HashMap<>();
    private static Map<Integer, Float> WG1 = new HashMap<>();

    private static float compute(String featureString,
                                 float kappa, float eta, float lambda, boolean isTrain) {
        float phi = 0.0f;
        int imbalanceTrainFactor = (kappa > 0) ? IMBALANCE_FACTOR : 1;

        String[] featureList = featureString.split(Constant.SPACE_SEPARATOR);
        List<FFMNode> ffmNodeList = new LinkedList<>();
        for (String feature : featureList) {
            ffmNodeList.add(new FFMNode(feature));
        }

        FFMNode[] ffmNodes = new FFMNode[ffmNodeList.size()];
        int j = 0;
        for (FFMNode ffmNode : ffmNodeList) {
            ffmNodes[j] = ffmNode;
            j++;
        }

        int normal = 1;

        for (int N1 = 0; N1 < ffmNodes.length - 1; N1++) {
            FFMNode nodeI = ffmNodes[N1];
            int f1 = nodeI.getField(); //field
            int j1 = nodeI.getFeature(); //feature
            float v1 = nodeI.getValue() / normal; //value
            if (f1 > Constant.FIELD_NUM) continue;

            if (isTrain) {
                boolean isAdaGrad = true; //modify to parameter
                float w = (W1.get(j1) == null ? 0.0f : W1.get(j1));
                if (isAdaGrad) {
                    float wg = (WG1.get(j1) == null ? 1.0f : WG1.get(j1));
                    WG1.put(j1, wg + kappa * kappa * v1 * v1);
                    w -= eta * v1 * kappa / (imbalanceTrainFactor * Math.sqrt(wg));
                } else {
                    w -= eta * v1 * kappa / (imbalanceTrainFactor);
                }
                W1.put(j1, w);
            } else {
                phi += (W1.get(j1) == null ? 0.0f : W1.get(j1)) * v1;
            }

            for (int N2 = N1 + 1; N2 < ffmNodes.length; N2++) {
                FFMNode nodeJ = ffmNodes[N2];
                int f2 = nodeJ.getField();
                int j2 = nodeJ.getFeature();
                float v2 = nodeJ.getValue() / normal;
                if (f2 > Constant.FIELD_NUM) continue;

                float v = v1 * v2;
                initLatentVector(j1, f2);
                initLatentVector(j2, f1);

                if (isTrain) {
                    float kappav = kappa * v;
                    Float[] newLatentVector1 = new Float[Constant.LATENT_VECTOR_DIM];
                    Float[] newLatentVector2 = new Float[Constant.LATENT_VECTOR_DIM];
                    Float[] newAccGradient1 = new Float[Constant.LATENT_VECTOR_DIM];
                    Float[] newAccGradient2 = new Float[Constant.LATENT_VECTOR_DIM];
                    for (int d = 0; d < Constant.LATENT_VECTOR_DIM; d++) {
                        float g1 = lambda * W.get(j1).getFieldLatentVectorMap().get(f2)[d] + kappav * W.get(j2).getFieldLatentVectorMap().get(f1)[d];
                        float g2 = lambda * W.get(j2).getFieldLatentVectorMap().get(f1)[d] + kappav * W.get(j1).getFieldLatentVectorMap().get(f2)[d];

                        float wg1 = W.get(j1).getFieldAccGradientMap().get(f2)[d] + g1 * g1 / imbalanceTrainFactor;
                        float wg2 = W.get(j2).getFieldAccGradientMap().get(f1)[d] + g2 * g2 / imbalanceTrainFactor;

                        float w1 = W.get(j1).getFieldLatentVectorMap().get(f2)[d] - (eta * g1) / (float) (Math.sqrt(wg1) * imbalanceTrainFactor);
                        newLatentVector1[d] = w1;
                        float w2 = W.get(j2).getFieldLatentVectorMap().get(f1)[d] - (eta * g2) / (float) (Math.sqrt(wg2) * imbalanceTrainFactor);
                        newLatentVector2[d] = w2;

                        newAccGradient1[d] = wg1;
                        newAccGradient2[d] = wg2;

                    }
                    W.get(j1).getFieldLatentVectorMap().put(f2, newLatentVector1);
                    W.get(j2).getFieldLatentVectorMap().put(f1, newLatentVector2);
                    W.get(j1).getFieldAccGradientMap().put(f2, newAccGradient1);
                    W.get(j2).getFieldAccGradientMap().put(f1, newAccGradient2);
                } else {
//                    计算\phi = \sum_i\sum_j(w_i*w_j*x_i*xj)，即各个特征的二项式组合值的求和。
                    for (int d = 0; d < Constant.LATENT_VECTOR_DIM; d++) {
                        phi += W.get(j1).getFieldLatentVectorMap().get(f2)[d] * W.get(j2).getFieldLatentVectorMap().get(f1)[d] * v;
                    }

                }
            }
        }

        return phi;
    }

    private static int getNormal(String featureString) {
        String[] featureList = featureString.split(Constant.SPACE_SEPARATOR);
        int normal = 0;
        for (String feature : featureList) {
            if (feature.contains("_") || feature.split(":")[0].endsWith("00") || feature.split(":")[0].length() > 6 || feature.split(":")[0].length() < 3 || feature.length() == 0)
                continue;
            normal++;
        }
        if (normal == 0) normal = 1;
        return normal;
    }




    public void train(String trainingDataDir, FFMParameter param) {
        LOG.info("Begin to train data in {}", trainingDataDir);
        long currentTime = System.currentTimeMillis();

        int count = 0;
        int negativeCount = 0; //debug log
        int positiveCount = 0;


        try {
            List<String> trainingDataFileList = HDFSUtils.getAllHdfsFile(trainingDataDir);
            Configuration conf = new Configuration();
            try (FileSystem fs = FileSystem.get(URI.create(trainingDataDir), conf)) {
                for (String trainingDataFile : trainingDataFileList) {
//                    LOG.info("Begion to train data in {}.", trainingDataFile); //debug log
                    try (FSDataInputStream hdfsInStream = fs.open(new Path(trainingDataFile));
                         BufferedReader in = new BufferedReader(new InputStreamReader(hdfsInStream, "UTF-8"));) {
                        String trainingData = "";
                        while ((trainingData = in.readLine()) != null) {

                            if (trainingData.split(TAB_SEPARATOR).length < 2 || trainingData.length() > 2000) continue;
                            float label = Float.parseFloat(trainingData.split(TAB_SEPARATOR)[2]);
                            String featureString = trainingData.split(TAB_SEPARATOR)[1];
                            float phi = compute(featureString, 0.0f, 0.0f, 0.0f, false);
//                            float expnyt = (float) Math.exp(-label * t);
//                            float kappa = -label * expnyt / (1 + expnyt);
                            if (label == 0) {
                                label = -1;
                            }
//                            float kappa = (sigmoid(phi) - label);
                            float kappa = -label / (1 + (float) Math.exp(label * phi));
                            compute(featureString, kappa, param.getEta(), param.getLambda(), true);
                            printCount("training", ++count);
//                            if (count > DEBUG_COUNT) break; //debug
                        }
                    }
//                    if (count > DEBUG_COUNT) break; //debug
                }
            }
        } catch (IOException e) {
            LOG.info("IOException occurs when reading training data.");
            e.printStackTrace();
        }
        LOG.info("It takes {} minutes to  train data in {}", (System.currentTimeMillis() - currentTime) / 60000, trainingDataDir);


//        return model;
    }


    public void predict(String toPredictDataDir, String predictResultFile) {
        LOG.info("Begin to predict data in dir: {}", toPredictDataDir);
        long currentTime = System.currentTimeMillis();
        int count = 0;
        try {
            Configuration conf = new Configuration();
            try (FileSystem fs = FileSystem.get(conf);
                 FSDataOutputStream fout = fs.create(new Path(predictResultFile));
                 BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));) {
                List<String> toPredictDataFileList = HDFSUtils.getAllHdfsFile(toPredictDataDir);
                for (String toPredictDataFile : toPredictDataFileList) {
                    try (FSDataInputStream hdfsInStream = fs.open(new Path(toPredictDataFile)); BufferedReader in = new BufferedReader(new InputStreamReader(hdfsInStream, "UTF-8"));) {
                        String toPredictData = "";
                        while ((toPredictData = in.readLine()) != null) {
                            if (toPredictData.split(TAB_SEPARATOR).length < 2 || toPredictData.length() > 2000)
                                continue;
                            String features = toPredictData.split(TAB_SEPARATOR)[1];

                            float phi = compute(features, 0.0f, 0.0f, 0.0f, false);

                            String predictResult = toPredictData.split(TAB_SEPARATOR)[0] + TAB_SEPARATOR + sigmoid(phi);
                            out.write(predictResult + "\n");
                            out.flush();
                            printCount("testing", ++count);
                            if (toPredictData.split(TAB_SEPARATOR).length > 2) {
                                rocUtils.updateROCn(Integer.parseInt(toPredictData.split(TAB_SEPARATOR)[2]), sigmoid(phi));
                            }

                        }
                    }
                }


            }
        } catch (IOException e) {
            LOG.info("IOException occurs when reading training data.");
            e.printStackTrace();
        }
        LOG.info("It takes {} minuts to  predict {}, ", (System.currentTimeMillis() - currentTime) / 60000, count);
        LOG.info("Predict result to {}.", predictResultFile);

    }

    public static void saveModel(String path) throws IOException {
        LOG.info("Begin to save model to {}", path);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(new File(path)), "UTF-8"));
        for (Map.Entry<Integer, Float> entry : W1.entrySet()) {
            bw.write(entry.getKey() + TAB_SEPARATOR + entry.getValue() + "\n");
        }

        for (Map.Entry<Integer, LatentVector> entry1 : W.entrySet()) {
            String line = entry1.getKey().toString() + Constant.COMMA_SEPARATOR + Constant.COMMA_SEPARATOR;
            for (Integer fields : entry1.getValue().getFieldLatentVectorMap().keySet()) {
                line += fields + Constant.COLON_SEPARATOR;
                String allFieldLatentVectorString = Arrays.toString(entry1.getValue().getFieldLatentVectorMap().get(fields));
                line += allFieldLatentVectorString.substring(1, allFieldLatentVectorString.length() - 1) + Constant.COLON_SEPARATOR;
                String allFieldAccGradientString = Arrays.toString(entry1.getValue().getFieldAccGradientMap().get(fields));
                line += allFieldAccGradientString.substring(1, allFieldAccGradientString.length() - 1) + Constant.COMMA_SEPARATOR + Constant.COMMA_SEPARATOR;
            }
            bw.write(line + "\n");
        }
        bw.close();
        LOG.info("Finish saving model to {}", path);
    }

    //sigmoid
    private static float sigmoid(float x) {
        return 1 / (1 + (float) Math.exp(-x));
    }

    private static void printCount(String type, int count) {
        if (count == 1 || count % 10000 == 0) {
            LOG.info("{} count:{}.", type, count);
        }
    }

    private static void initLatentVector(int j1, int f2) {
        if (null == W.get(j1)) {
            W.put(j1, new LatentVector(f2));
        } else if (null == W.get(j1).getFieldLatentVectorMap().get(f2)) {
            LatentVector tmpLatentVector = W.get(j1);
            tmpLatentVector.getFieldLatentVectorMap().put(f2, LatentVector.getInitLatentVector());
            tmpLatentVector.getFieldAccGradientMap().put(f2, LatentVector.getInitAccGradient());
            W.put(j1, tmpLatentVector);
        }
    }

    public static void main(String[] args) throws IOException {


        FFM ffmAlgorithm = new FFM();
        String trainingDataDir = args[0];
        String toPredictDataDir = args[1];
        String predictResultDir = args[2];

        FFMParameter param = new FFMParameter();

        ffmAlgorithm.train(trainingDataDir, param);
        LOG.info("Feature count: {}.", W.keySet().size());
//        saveModel("");
        ffmAlgorithm.predict(toPredictDataDir, predictResultDir);
        rocUtils.printROC("/tmp/roctmp.txt");
    }

}



