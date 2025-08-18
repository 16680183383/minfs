package com.ksyun.campus.metaserver.bootstrap;

import com.ksyun.campus.metaserver.services.ReplicationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class StartupSyncRunner implements ApplicationRunner {

	@Autowired
	private ReplicationService replicationService;

	@Override
	public void run(ApplicationArguments args) {
		try {
			log.info("启动后检查并尝试从Leader追赶快照...");
			replicationService.catchUpFromLeaderIfNeeded();
		} catch (Exception e) {
			log.warn("启动期追赶失败: {}", e.getMessage());
		}
	}
}


