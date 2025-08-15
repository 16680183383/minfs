package com.example.dataserver.controller;

import com.example.dataserver.services.DataService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController("/")
public class DataController {

    @Autowired
    private DataService dataService;

    /**
     * 1、读取request content内容并保存在本地磁盘下的文件内
     * 2、同步调用其他ds服务的write，完成另外2副本的写入
     * 3、返回写成功的结果及三副本的位置
     * @param fileSystemName
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping(value = "write", method = RequestMethod.POST)
    public ResponseEntity<Map<String, Object>> writeFile(
            @RequestHeader String fileSystemName,
            @RequestParam String path,
            @RequestParam int offset,
            @RequestParam int length,
            @RequestHeader(value = "X-Is-Replica-Sync", required = false) String xIsReplicaSync,
            HttpServletRequest request) {
        try {
            // 读取请求体中的二进制数据
            byte[] data = request.getInputStream().readAllBytes();

            // 核心修改：判断是否为副本同步请求（头存在且为"true"）
            boolean isReplicaSync = "true".equalsIgnoreCase(xIsReplicaSync);
            // 调用服务层方法时传入标识
            List<String> replicaLocations = dataService.writeWithChunk(data, path, isReplicaSync);

            // 构造返回结果：成功标识+三副本位置
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("replicaLocations", replicaLocations);
            result.put("message", "Write success");
            return new ResponseEntity<>(result, HttpStatus.OK);
        } catch (Exception e) {
            String errorMsg = "写入失败：" + e.getMessage();
            if (e.getMessage().contains("Not enough available dataServer")) {
                errorMsg = "写入失败：可用 dataServer 不足，无法满足三副本要求";
            } else if (e.getMessage().contains("Failed to sync replica")) {
                errorMsg = "写入失败：副本同步失败";
            }
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", errorMsg);
            return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 在指定本地磁盘路径下，读取指定大小的内容后返回
     * @param fileSystemName
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping(value = "read", method = RequestMethod.GET)
    public ResponseEntity<byte[]> readFile(
            @RequestHeader String fileSystemName,
            @RequestParam String path,
            @RequestParam int offset,
            @RequestParam int length) {
        try {
            // 调用服务层读取数据
            byte[] data = dataService.read(path, offset, length);

            // 若数据为空，返回404
            if (data == null) {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }

            // 返回二进制数据
            return new ResponseEntity<>(data, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage().getBytes(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }

    // 每个dataServer需实现/checkFileExists接口（Controller层）
    @RequestMapping(value = "checkFileExists", method = RequestMethod.GET)
    public ResponseEntity<Boolean> checkFileExists(
            @RequestParam String path,
            @RequestHeader String fileSystemName) {
        try {
            String localChunkPath = dataService.getLocalFilePath(path + "/chunk_0"); // 检查第一个块（简化）
            boolean exists = new File(localChunkPath).exists();
            return ResponseEntity.ok(exists);
        } catch (Exception e) {
            return ResponseEntity.ok(false);
        }
    }
}
