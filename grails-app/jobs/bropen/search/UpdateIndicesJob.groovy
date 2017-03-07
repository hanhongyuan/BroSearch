/* Copyright © 2016 北京博瑞开源软件有限公司版权所有。*/
package bropen.search

import bropen.framework.core.Locker
import bropen.framework.plugins.quartz.QuartzJob
import bropen.search.es.EsCoreService

/**
 * 增量更新索引
 */
class UpdateIndicesJob {

	def scope = QuartzJob.SCOPE_APP
	def lock = QuartzJob.LOCK_APP
	def group = "BroSearch"

	def timeout = 3600000l    // execute job once in 1h
	def concurrent = true    // 同时可以运行多个实例，用lock来避免冲突
	def durability = true    // scheduler shutdown之后，把job保存到数据库中

	UserIndexService userIndexService
	SearchableIndexService searchableIndexService
	AttachmentIndexService attachmentIndexService

	static triggers = {
		cron name: 'cronTriggerImportJob', startDelay: 10000, cronExpression: '0 5 * * * ?'
	}

	def execute() {
		if (!EsCoreService.enabled()) return
		if (Locker.isLocked(Constants.INIT_LOCK, false)) return
		userIndexService.update()
		searchableIndexService.update()
		attachmentIndexService.update()
	}

}