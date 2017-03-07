/* Copyright © 2016 北京博瑞开源软件有限公司版权所有。*/
package update

import bropen.framework.core.DataDict
import bropen.framework.core.SettingService
import bropen.search.Constants

class BroSearch000Updater {

	def initiator = true
	def version = "4.1.0.22715"

	SettingService settingService

	@grails.transaction.Transactional
	def beforeBootStrap() {
		// es相关配置
		settingService.createOrUpdate("bropen.search.ElasticSearch.hosts", null, "string", null, null,
				[notes: "ES服务器地址（如 127.0.0.1:9300），可以用分号“;”分隔多个节点的地址；注意这里的端口不是http端口。"])
		settingService.createOrUpdate("bropen.search.ElasticSearch.cluster.name", "bropen", null, null, null,
				[notes: "ES集群名称"])
		settingService.createOrUpdate("bropen.search.ElasticSearch.page.import.size", 1000, null, null, null,
				[notes: "ES批量索引时的每页数据条数"])
		settingService.createOrUpdate("bropen.search.ElasticSearch.page.query.size", 20, null, null, null,
				[notes: "ES查询时的每页条数"])
		settingService.createOrUpdate("bropen.search.ElasticSearch.login.username", null, null, null, null,
				[notes: "ES服务器的登录用户名"])
		settingService.createOrUpdate("bropen.search.ElasticSearch.login.password", null, null, null, null,
				[notes: "ES服务器的登录密码"])

		// 附件解析相关配置
		settingService.createOrUpdate("bropen.search.url.file.parse",
				"http://127.0.0.1:9080/fileparser/default", null, null, null,
				[notes: "附件内容解析服务器的地址，含仓库名称。"])

		// 索引分类
		DataDict.createDict(
				"文档分类", Constants.DD_CATEGORIES,
				[group       : "搜索引擎",
				 hierarchical: true,
				 valKeyName  : "代码",
				 valName     : "名称",
				 extName1    : "是否切面",
				 extType1    : DataDict.TYPE_BOOL,
				 extName2    : "是否主分类",
				 extType2    : DataDict.TYPE_BOOL,
				 notes       : "切面表示子分类通过数据聚合自动生成，此时必须配置代码以便二次开发调用；\n主分类会显示在搜索框。"]).save(flush: true)
	}

}
