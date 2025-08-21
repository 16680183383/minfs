package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;

import java.io.IOException;
import java.util.List;

/**
 * 基类，定义了通用的文件系统方法和变量
 * 整体的文件组织结构为以下形式
 * {namespace}/{dir}
 *                  /{subdir}
 *                  /{subdir}/file
 *                  /file
 */
public abstract class FileSystem {

    // 文件系统名称，可理解成命名空间
    protected String defaultFileSystemName;

    // 抽象接口，由具体实现类完成
    public abstract FSInputStream open(String path) throws IOException;

    public abstract FSOutputStream create(String path) throws IOException;

    public abstract boolean mkdir(String path) throws IOException;

    public abstract boolean delete(String path) throws IOException;

    public abstract StatInfo getFileStats(String path) throws IOException;

    public abstract List<StatInfo> listFileStats(String path) throws IOException;

    public abstract ClusterInfo getClusterInfo() throws IOException;

    public abstract boolean exists(String path) throws IOException;
}
