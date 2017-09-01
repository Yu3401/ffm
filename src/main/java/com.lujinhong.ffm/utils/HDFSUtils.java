package com.lujinhong.ffm.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * AUTHOR: LUJINHONG
 * CREATED ON: 17/1/9 17:24
 * PROJECT NAME: etl_hadoop_aplus
 * DESCRIPTION:
 */
public class HDFSUtils {
    private static Logger LOG = LoggerFactory.getLogger(HDFSUtils.class);

    public static List<String> getFileNameList(String dir) throws IOException {
        List<String> list = new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] stats = fs.listStatus(new Path(dir));
        for (int i = 0; i < stats.length; ++i) {
            if (!stats[i].isDirectory()) {
                // regular file
                list.add(stats[i].getPath().toString());
                //LOG.debug("Load file : {}", stats[i].getPath().toString());
            } else {
                // dir
                LOG.info("Ignore directory : {}", stats[i].getPath().toString());
            }
        }
        fs.close();
        return list;
    }

    /**
     * 递归删除文件或目录，或者其集合
     * @param fileNameSet
     */
    public static void  deleteFile(Set<String> fileNameSet){
        //多次构建FileSystem对象，如果调用很频繁的话考虑用单例。
        for(String fileName : fileNameSet){
            deleteFile(fileName);
        }
    }
    public static void  deleteFile(String fileName){
        try(FileSystem fs = FileSystem.get(URI.create(fileName),new Configuration())){
            if(fs.exists(new Path(fileName))){
                fs.delete(new Path(fileName),true);
                LOG.info("{} deleted.",fileName);
            }
        }catch (IOException e){
            LOG.info("Error happens when deleting file: {}.", fileName);
            e.printStackTrace();
        }

    }

    //递归列出目录中的所有文件。
    public static List<String> getAllHdfsFile(String dir) throws IOException {
        List<String> fileList = new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(
                new Path(dir), true);

        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            fileList.add(fileStatus.getPath().toString());
        }
        return fileList;

    }


    /**
     * 文件检测并删除
     *
     * @param path
     * @param conf
     * @return
     */
    public static boolean checkAndDel(final String path, Configuration conf) {
        Path dstPath = new Path(path);
        try {
            FileSystem fs = dstPath.getFileSystem(conf);
            if (fs.exists(dstPath)) {
                fs.delete(dstPath, true);
            } else {
                return false;
            }
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean checkHdfsFiledir(final String path, Configuration conf) {
        Path dstPath = new Path(path);
        try {
            FileSystem dhfs = dstPath.getFileSystem(conf);
            if (dhfs.exists(dstPath)) {
                return true;
            } else {
                return false;
            }
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
    }

}
