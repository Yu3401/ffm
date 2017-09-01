package com.lujinhong.ffm.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

/**
 * AUTHOR: LUJINHONG
 * CREATED ON: 17/6/13 10:20
 * PROJECT NAME: aplus_dmp
 * DESCRIPTION:
 */
public class ROCUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ROCUtils.class);

    private static final int SPLIT_SIZE = 100;//将预测概率值从0～1分配成100个区间。
    private int Pn[] = new int[SPLIT_SIZE];//记录分配到某个区间的正样本的数量。
    private int Nn[] = new int[SPLIT_SIZE];//分配到某个区间的负样本的数量。

    public void updateROCn(int label, double predict) {
        int dn = (int) (predict * 100);
        if (dn >= 100) {
            dn = SPLIT_SIZE - 1;
        } else if (dn <= 0) {
            dn = 0;
        }

        if (label == 0) {
            Nn[dn]++;
        } else if (label == 1) {
            Pn[dn]++;
        }
    }

    //保存ROC与AUC，写到HDFS及console。
    public void printROC(String rocFilePath) {
        //负样本总数
        int sumN = 0;
        for (int count : Nn) {
            sumN += count;
        }
        //正样本总数
        int sumP = 0;
        for (int count : Pn) {
            sumP += count;
        }
        int TP[] = new int[SPLIT_SIZE];
        int FP[] = new int[SPLIT_SIZE];

        TP[SPLIT_SIZE - 1] = Pn[SPLIT_SIZE - 1];
        for (int i = SPLIT_SIZE - 2; i >= 0; i--) {
            TP[i] = Pn[i] + TP[i + 1];
        }

        FP[SPLIT_SIZE - 1] = Nn[SPLIT_SIZE - 1];
        for (int i = SPLIT_SIZE - 2; i >= 0; i--) {
            FP[i] = Nn[i] + FP[i + 1];
        }
        double auc = 0.0;

        //标准输出
        System.out.println("ROC result:");

        if (sumP == 0) { //没有正样本的情况，避免分母为0
            for (int i = 99; i >= 0; i--) {
                System.out.println(i + "\t" + (double) FP[i] / sumN + "\t" + "0.0");
            }
            auc = 0.0;
        } else {
            for (int i = 99; i >= 0; i--) {
                System.out.println(i + "\t" + (double) FP[i] / sumN + "\t" + (double) TP[i] / sumP);
                //计算AUC
                if (i == 99) {
                    auc += ((double) TP[i] / sumN) * ((double) FP[i] / sumP);
                } else {
                    auc += ((double) TP[i] / sumN) * ((double) FP[i] / sumP - (double) FP[i + 1] / sumP);
                }
            }
        }

        LOG.info("AUC  is {}: ", auc);
        //标准输出，方便调度器上查看。
        System.out.printf("AUC  is " +  auc);
        System.out.println("=========================================================");

        //保存到HDFS
        Configuration conf = new Configuration();

        try (FileSystem fs = FileSystem.get(conf);
             FSDataOutputStream fout = fs.create(new Path(rocFilePath));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));) {
            out.write(auc + "\n");
            if (sumP == 0) { //没有正样本的情况，避免分母为0
                for (int i = 99; i >= 0; i--) {
                    out.write(i + "\t" + (double) FP[i] / sumN + "\t" + "0.0" + "\n");
                }
            }else {
                for (int i = 99; i >= 0; i--) {
                    out.write(i + "\t" + (double) FP[i] / sumN + "\t" + (double) TP[i] / sumP + "\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Save ROC and AUC to {}.", rocFilePath);

    }
}
