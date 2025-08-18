package com.example.dataserver.controller;

import com.example.dataserver.services.DataService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
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
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping(value = "write", method = RequestMethod.POST)
    public ResponseEntity<Map<String, Object>> writeFile(
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
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping(value = "read", method = RequestMethod.GET)
    public ResponseEntity<byte[]> readFile(
            @RequestParam String path,
            @RequestParam int offset,
            @RequestParam int length) {
        try {
            // 调用服务层读取数据
            byte[] data = dataService.readWithChunk(path);

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
            @RequestParam String path) {
        try {
            String localChunkPath = dataService.getLocalFilePath(path + "/chunk_0"); // 检查第一个块（简化）
            boolean exists = new File(localChunkPath).exists();
            return ResponseEntity.ok(exists);
        } catch (Exception e) {
            return ResponseEntity.ok(false);
        }
    }

    /**
     * 副本节点接收MD5清单并写入本地
     * 接口功能：接收主节点同步的MD5清单数据，生成本地MD5清单文件（与主节点路径一致）
     * @param path MD5清单的目标路径（如/test/4.txt/md5_list.txt）
     * @param request 用于读取请求体中的MD5清单二进制数据
     * @return 写入结果
     */
    @RequestMapping(value = "writeMd5List", method = RequestMethod.POST)
    public ResponseEntity<Map<String, Object>> writeMd5List(
            @RequestParam String path,
            @RequestHeader(required = false) String xIsReplicaSync,
            HttpServletRequest request) {
        try {
            // 1. 读取请求体中的MD5清单数据（主节点同步过来的二进制数据）
            byte[] md5ListData = request.getInputStream().readAllBytes();

            // 2. 调用服务层方法，将MD5清单写入本地指定路径
            // （路径格式与主节点一致，确保后续读取时能匹配）
            dataService.writeMd5ListToLocal(path, md5ListData == null ? new byte[0] : md5ListData);

            // 3. 构造成功响应（符合文档"接口需反馈结果"的隐含要求）
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("message", "MD5清单写入成功");
            result.put("md5ListPath", path);
            return new ResponseEntity<>(result, HttpStatus.OK);

        } catch (IOException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", "MD5清单写入失败：" + e.getMessage());
            return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", "MD5清单处理失败：" + e.getMessage());
            return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "delete", method = RequestMethod.DELETE)
    public ResponseEntity<Map<String, Object>> deleteFile(
            @RequestParam String path,
            @RequestHeader(value = "X-Is-Replica-Sync", required = false) String xIsReplicaSync) {
        try {
            // 判断是否为副本同步请求
            boolean isReplicaSync = "true".equalsIgnoreCase(xIsReplicaSync);

            // 调用DataService删除本地文件
            boolean deleted = dataService.deleteFileLocally(path);

            Map<String, Object> result = new HashMap<>();
            result.put("success", deleted);
            result.put("message", deleted ? "Delete success" : "Delete failed");
            return new ResponseEntity<>(result, HttpStatus.OK);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", "删除失败：" + e.getMessage());
            return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
