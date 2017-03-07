/* Copyright © 2016 北京博瑞开源软件有限公司版权所有。*/
package update

import bropen.framework.core.DataDict
import bropen.framework.core.Setting
import bropen.framework.core.SettingService
import bropen.search.Constants

class BroSearch481Updater {

	def version = "4.8.1.23550"
	def description = "4.8.1 升级器"

	SettingService settingService

	@grails.transaction.Transactional
	def beforeBootStrap() {
		beforeBootStrap23463()
		beforeBootStrap23548()
	}

	private void beforeBootStrap23463() {
		if (version("beforeBootStrap", ">=", "4.8.1.23463")) return
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

	private void beforeBootStrap23548() {
		if (version("beforeBootStrap", ">=", "4.8.1.23548")) return
		Setting setting = bropen.framework.core.Setting.findByCode("bropen.search.url.file.parse")
		if (setting?.value?.contains("/BroFileParser/parse")) {
			setting.value = setting.value.replace("/BroFileParser/parse", "/fileparser/default")
			setting.notes = "附件内容解析服务器的地址，含仓库名称。"
			setting.save(flush: true)
		}
	}

}