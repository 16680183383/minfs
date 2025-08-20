package com.ksyun.campus.demoweb.api;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.FSInputStream;
import com.ksyun.campus.client.FSOutputStream;
import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@RestController
@RequestMapping("/api/fs")
public class FsController {

    // 移除固定的文件系统实例，改为动态创建
    // private final EFileSystem fs = new EFileSystem("demo");

    /**
     * 根据文件系统名称获取或创建EFileSystem实例
     */
    private EFileSystem getFileSystem(String fileSystemName) {
        if (fileSystemName == null || fileSystemName.trim().isEmpty()) {
            fileSystemName = "default"; // 默认文件系统
        }
        return new EFileSystem(fileSystemName.trim());
    }

    @PostMapping("/mkdir")
    public Map<String, Object> mkdir(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName, 
                                    @RequestParam String path) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        boolean ok = fs.mkdir(path);
        Map<String, Object> r = new HashMap<>();
        r.put("success", ok);
        r.put("fileSystemName", fs.getFileSystemName());
        return r;
    }

    @PostMapping("/create")
    public Map<String, Object> create(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName, 
                                     @RequestParam String path) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        FSOutputStream out = fs.create(path);
        out.close();
        Map<String, Object> r = new HashMap<>();
        r.put("success", true);
        r.put("fileSystemName", fs.getFileSystemName());
        return r;
    }

    @DeleteMapping("/delete")
    public Map<String, Object> delete(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName, 
                                     @RequestParam String path) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        boolean ok = fs.delete(path);
        Map<String, Object> r = new HashMap<>();
        r.put("success", ok);
        r.put("fileSystemName", fs.getFileSystemName());
        return r;
    }

    @GetMapping("/stat")
    public StatInfo stat(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName, 
                        @RequestParam String path) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        return fs.getFileStats(path);
    }

    @GetMapping("/list")
    public List<StatInfo> list(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName, 
                               @RequestParam String path) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        return fs.listFileStats(path);
    }

    @PostMapping(value = "/write", consumes = MediaType.TEXT_PLAIN_VALUE)
    public Map<String, Object> write(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName, 
                                    @RequestParam String path, @RequestBody String content) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        FSOutputStream out = fs.create(path);
        out.write(content.getBytes(StandardCharsets.UTF_8));
        out.close();
        Map<String, Object> r = new HashMap<>();
        r.put("success", true);
        r.put("size", content.getBytes(StandardCharsets.UTF_8).length);
        r.put("fileSystemName", fs.getFileSystemName());
        return r;
    }

    @GetMapping(value = "/read", produces = MediaType.TEXT_PLAIN_VALUE)
    public String read(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName, 
                       @RequestParam String path) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        FSInputStream in = fs.open(path);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
            bos.write(buf, 0, n);
        }
        in.close();
        return bos.toString(StandardCharsets.UTF_8);
    }

    @GetMapping("/exists")
    public Map<String, Object> exists(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName, 
                                     @RequestParam String path) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        boolean ok = fs.exists(path);
        Map<String, Object> r = new HashMap<>();
        r.put("exists", ok);
        r.put("fileSystemName", fs.getFileSystemName());
        return r;
    }

    @GetMapping("/cluster")
    public ClusterInfo cluster(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        return fs.getClusterInfo();
    }

    /**
     * 列出所有文件系统名称（透传 MetaServer 的 /filesystems）
     */
    @GetMapping("/filesystems")
    public List<String> listFileSystems(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName) throws IOException {
        EFileSystem fs = getFileSystem(fileSystemName);
        // 直接请求 MetaServer 的 /filesystems
        String leader = fs.getMetaServerAddress();
        String url = "http://" + leader + "/filesystems";
        String resp = com.ksyun.campus.client.util.HttpClientUtil.doGet(fs.getHttpClient(), url);
        if (resp == null || resp.contains("error")) {
            return java.util.Collections.emptyList();
        }
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        return mapper.readValue(resp, new com.fasterxml.jackson.core.type.TypeReference<List<String>>(){});
    }

    @PostMapping("/testWriteRead")
    public Map<String, Object> testWriteRead(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName, 
                                            @RequestParam String path, @RequestParam long sizeBytes) throws Exception {
        EFileSystem fs = getFileSystem(fileSystemName);
        byte[] data = generateDeterministicData(sizeBytes);
        String md5Write = md5Hex(data);
        long t1 = System.currentTimeMillis();
        FSOutputStream out = fs.create(path);
        out.write(data);
        out.close();
        long writeMs = System.currentTimeMillis() - t1;

        long t2 = System.currentTimeMillis();
        FSInputStream in = fs.open(path);
        ByteArrayOutputStream bos = new ByteArrayOutputStream((int)Math.min(sizeBytes, 16 * 1024 * 1024));
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) bos.write(buf, 0, n);
        in.close();
        byte[] read = bos.toByteArray();
        String md5Read = md5Hex(read);
        long readMs = System.currentTimeMillis() - t2;

        Map<String, Object> r = new HashMap<>();
        r.put("sizeBytes", sizeBytes);
        r.put("md5Write", md5Write);
        r.put("md5Read", md5Read);
        r.put("equal", md5Write.equals(md5Read));
        r.put("writeMs", writeMs);
        r.put("readMs", readMs);
        r.put("fileSystemName", fs.getFileSystemName());
        return r;
    }

    @PostMapping("/fsck")
    public Map<String, Object> triggerFsck(@RequestHeader(value = "fileSystemName", required = false) String fileSystemName) {
        Map<String, Object> r = new HashMap<>();
        try {
            EFileSystem fs = getFileSystem(fileSystemName);
            String meta = fs.getMetaServerAddress();
            com.ksyun.campus.client.util.HttpClientUtil.doGet(fs.getHttpClient(), "http://" + meta + "/fsck/manual");
            r.put("success", true);
            r.put("fileSystemName", fs.getFileSystemName());
        } catch (Exception e) {
            r.put("success", false);
            r.put("error", e.getMessage());
        }
        return r;
    }

    private static byte[] generateDeterministicData(long size) {
        byte[] b = new byte[(int) size];
        // 生成可复现内容：使用 ASCII 字符 'A'，避免 UTF-8 编解码带来的差异
        for (int i = 0; i < b.length; i++) b[i] = (byte) 'A';
        return b;
    }

    private static String md5Hex(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);
        StringBuilder sb = new StringBuilder();
        for (byte value : digest) sb.append(String.format("%02x", value));
        return sb.toString();
    }
}


