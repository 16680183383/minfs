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

    private final EFileSystem fs = new EFileSystem("demo");

    @PostMapping("/mkdir")
    public Map<String, Object> mkdir(@RequestParam String path) throws IOException {
        boolean ok = fs.mkdir(path);
        Map<String, Object> r = new HashMap<>();
        r.put("success", ok);
        return r;
    }

    @PostMapping("/create")
    public Map<String, Object> create(@RequestParam String path) throws IOException {
        FSOutputStream out = fs.create(path);
        out.close();
        Map<String, Object> r = new HashMap<>();
        r.put("success", true);
        return r;
    }

    @DeleteMapping("/delete")
    public Map<String, Object> delete(@RequestParam String path) throws IOException {
        boolean ok = fs.delete(path);
        Map<String, Object> r = new HashMap<>();
        r.put("success", ok);
        return r;
    }

    @GetMapping("/stat")
    public StatInfo stat(@RequestParam String path) throws IOException {
        return fs.getFileStats(path);
    }

    @GetMapping("/list")
    public List<StatInfo> list(@RequestParam String path) throws IOException {
        return fs.listFileStats(path);
    }

    @PostMapping(value = "/write", consumes = MediaType.TEXT_PLAIN_VALUE)
    public Map<String, Object> write(@RequestParam String path, @RequestBody String content) throws IOException {
        FSOutputStream out = fs.create(path);
        out.write(content.getBytes(StandardCharsets.UTF_8));
        out.close();
        Map<String, Object> r = new HashMap<>();
        r.put("success", true);
        r.put("size", content.getBytes(StandardCharsets.UTF_8).length);
        return r;
    }

    @GetMapping(value = "/read", produces = MediaType.TEXT_PLAIN_VALUE)
    public String read(@RequestParam String path) throws IOException {
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
    public Map<String, Object> exists(@RequestParam String path) throws IOException {
        boolean ok = fs.exists(path);
        Map<String, Object> r = new HashMap<>();
        r.put("exists", ok);
        return r;
    }

    @GetMapping("/cluster")
    public ClusterInfo cluster() throws IOException {
        return fs.getClusterInfo();
    }

    @PostMapping("/testWriteRead")
    public Map<String, Object> testWriteRead(@RequestParam String path, @RequestParam long sizeBytes) throws Exception {
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
        return r;
    }

    @PostMapping("/fsck")
    public Map<String, Object> triggerFsck() {
        Map<String, Object> r = new HashMap<>();
        try {
            String meta = fs.getMetaServerAddress();
            com.ksyun.campus.client.util.HttpClientUtil.doGet(fs.getHttpClient(), "http://" + meta + "/fsck/manual");
            r.put("success", true);
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


