package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.ReplicationType;
import com.ksyun.campus.metaserver.domain.StatInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ReplicationService {

	@Autowired
	private RestTemplate restTemplate;

	@Autowired
	private ZkMetaServerService zkMetaServerService;

	@Autowired
	private MetadataStorageService metadataStorageService;

	// follower启动时或掉线重连后调用：从leader拉取快照
	public void catchUpFromLeaderIfNeeded() {
		try {
			if (zkMetaServerService.isLeader()) {
				return; // 主节点无需追赶
			}
			String leader = zkMetaServerService.getLeaderAddress();
			if (leader == null) {
				return;
			}
			String url = "http://" + leader + "/internal/snapshot";
			ResponseEntity<Map> resp = restTemplate.exchange(url, HttpMethod.GET, HttpEntity.EMPTY, Map.class);
			Object list = resp.getBody() != null ? resp.getBody().get("files") : null;
			if (list instanceof List) {
				@SuppressWarnings("unchecked") List<Map<String, Object>> files = (List<Map<String, Object>>) list;
				java.util.Set<String> snapshotPaths = new java.util.HashSet<>();
				int filesWithReplicas = 0;
				for (Map<String, Object> f : files) {
					String path = String.valueOf(f.get("path"));
					String type = String.valueOf(f.get("type"));
					String fileSystemName = String.valueOf(f.getOrDefault("fileSystemName", "default"));
					Number sizeNum = (Number) f.getOrDefault("size", 0);
					StatInfo info = new StatInfo();
					info.setPath(path);
					info.setType("Directory".equalsIgnoreCase(type) ? FileType.Directory : FileType.File);
					info.setSize(sizeNum.longValue());
					info.setMtime(System.currentTimeMillis());
					Object reps = f.get("replicas");
					if (reps instanceof java.util.List<?> repList) {
						java.util.List<ReplicaData> replicaDataList = new java.util.ArrayList<>();
						for (Object o : repList) {
							if (o instanceof java.util.Map<?,?> m) {
								ReplicaData r = new ReplicaData();
								r.id = String.valueOf(m.get("id"));
								r.dsNode = String.valueOf(m.get("dsNode"));
								Object pathValue = m.get("path");
								r.path = String.valueOf(pathValue != null ? pathValue : path);
								Object off = m.get("offset");
								Object len = m.get("length");
								r.offset = off instanceof Number ? ((Number) off).intValue() : 0;
								r.length = len instanceof Number ? ((Number) len).intValue() : 0;
								Object pri = m.get("isPrimary");
								r.isPrimary = pri instanceof Boolean ? (Boolean) pri : false;
								replicaDataList.add(r);
							}
						}
						info.setReplicaData(replicaDataList);
						if (!replicaDataList.isEmpty()) {
							filesWithReplicas++;
						}
					}
					metadataStorageService.saveMetadata(fileSystemName, path, info);
					snapshotPaths.add(fileSystemName + ":" + path);
				}
				// 删除本地存在但不在快照中的路径
				List<StatInfo> localAll = metadataStorageService.getAllMetadata();
				for (StatInfo s : localAll) {
					String key = "default:" + s.getPath(); // 默认文件系统
					if (!snapshotPaths.contains(key)) {
						metadataStorageService.deleteMetadata("default", s.getPath());
					}
				}
				log.info("快照同步完成，共 {} 条，其中包含副本信息的文件: {}", ((List<?>) list).size(), filesWithReplicas);
			}
		} catch (Exception e) {
			log.error("从Leader拉取快照失败", e);
		}
	}

	public void replicateToFollowers(ReplicationType type, String path, Map<String, Object> payload) {
		try {
			if (!zkMetaServerService.isLeader()) {
				// 非主节点不负责下发复制
				return;
			}
			List<Map<String, Object>> metas = zkMetaServerService.getAllMetaServers();
			Map<String, Object> selfInfo = zkMetaServerService.getCurrentMetaServerInfo();
			String self = (selfInfo.get("host") != null && selfInfo.get("port") != null)
					? selfInfo.get("host") + ":" + selfInfo.get("port")
					: String.valueOf(selfInfo.get("address"));
			List<String> followers = metas.stream()
					.filter(m -> !self.equals(String.valueOf(m.get("address"))))
					.map(m -> String.valueOf(m.get("address")))
					.toList();
			for (String follower : followers) {
				String url = "http://" + follower + "/internal/replicate?type=" + type + "&path=" + path;
				ResponseEntity<String> resp = restTemplate.exchange(url, HttpMethod.POST, new HttpEntity<>(payload), String.class);
				log.info("向从节点 {} 复制 {} 成功, path={},结果 {}", follower, type, path,resp);
			}
		} catch (Exception e) {
			log.error("复制到从节点失败: type={}, path={}", type, path, e);
		}
	}

	// 被动接收复制（由Follower调用）
	public boolean applyReplication(ReplicationType type, String path, Map<String, Object> payload) {
		try {
			String fileSystemName = String.valueOf(payload.getOrDefault("fileSystemName", "default"));
			switch (type) {
				case CREATE_FILE -> {
					StatInfo info = new StatInfo();
					info.setPath(path);
					info.setType(FileType.File);
					info.setSize(((Number) payload.getOrDefault("size", 0)).longValue());
					info.setMtime(System.currentTimeMillis());
					metadataStorageService.saveMetadata(fileSystemName, path, info);
				}
				case CREATE_DIR -> {
					StatInfo info = new StatInfo();
					info.setPath(path);
					info.setType(FileType.Directory);
					info.setSize(0);
					info.setMtime(System.currentTimeMillis());
					metadataStorageService.saveMetadata(fileSystemName, path, info);
				}
				case WRITE -> {
					StatInfo existing = metadataStorageService.getMetadata(fileSystemName, path);
					if (existing == null) {
						existing = new StatInfo();
						existing.setPath(path);
						existing.setType(FileType.File);
					}
					long size = ((Number) payload.getOrDefault("size", existing.getSize())).longValue();
					existing.setSize(size);
					existing.setMtime(System.currentTimeMillis());
					Object reps = payload.get("replicas");
					if (reps instanceof java.util.List<?> list) {
						java.util.List<ReplicaData> replicaDataList = new java.util.ArrayList<>();
						for (Object o : list) {
							if (o instanceof java.util.Map<?,?> m) {
								ReplicaData r = new ReplicaData();
								r.id = String.valueOf(m.get("id"));
								r.dsNode = String.valueOf(m.get("dsNode"));
								Object pathValue = m.get("path");
								r.path = String.valueOf(pathValue != null ? pathValue : path);
								Object off = m.get("offset");
								Object len = m.get("length");
								r.offset = off instanceof Number ? ((Number) off).intValue() : 0;
								r.length = len instanceof Number ? ((Number) len).intValue() : 0;
								Object pri = m.get("isPrimary");
								r.isPrimary = pri instanceof Boolean ? (Boolean) pri : false;
								replicaDataList.add(r);
							}
						}
						existing.setReplicaData(replicaDataList);
					}
					metadataStorageService.saveMetadata(fileSystemName, path, existing);
				}
				case DELETE -> {
					StatInfo existing = metadataStorageService.getMetadata(fileSystemName, path);
					if (existing == null) {
						List<StatInfo> children = metadataStorageService.listDirectory(fileSystemName, path);
						if (children != null && !children.isEmpty()) {
							deleteDirectoryRecursively(fileSystemName, path);
						} else {
							metadataStorageService.deleteMetadata(fileSystemName, path);
						}
					} else if (existing.getType() == FileType.Directory) {
						deleteDirectoryRecursively(fileSystemName, path);
					} else {
						metadataStorageService.deleteMetadata(fileSystemName, path);
					}
				}
				// 去除RENAME分支
				default -> {
				}
			}
			return true;
		} catch (Exception e) {
			log.error("应用复制失败: type={}, path={}", type, path, e);
			return false;
		}
	}

	private void deleteDirectoryRecursively(String fileSystemName, String dirPath) {
		List<StatInfo> children = metadataStorageService.listDirectory(fileSystemName, dirPath);
		for (StatInfo child : children) {
			String childPath = child.getPath();
			if (child.getType() == FileType.Directory) {
				deleteDirectoryRecursively(fileSystemName, childPath);
			} else {
				metadataStorageService.deleteMetadata(fileSystemName, childPath);
			}
		}
		metadataStorageService.deleteMetadata(fileSystemName, dirPath);
	}
}


