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
			int attempts = 0;
			boolean done = false;
			while (attempts < 8 && !done) {
				attempts++;
				try {
					replicationService.catchUpFromLeaderIfNeeded();
					// 如果不是Leader且拿到了快照，会在服务内部完成；这里无返回值，只做重试时机
					done = true; // 若第一次没有拿到Leader也不致命，下面sleep后会再试
				} catch (Exception inner) {
					log.warn("第{}次启动期追赶失败: {}", attempts, inner.getMessage());
				}
				if (!done) {
					try { Thread.sleep(2000); } catch (InterruptedException ignored) { }
				}
			}
		} catch (Exception e) {
			log.warn("启动期追赶流程异常: {}", e.getMessage());
		}
	}
}


