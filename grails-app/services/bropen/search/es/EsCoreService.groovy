/* Copyright © 2016 北京博瑞开源软件有限公司版权所有。*/
package bropen.search.es

import bropen.framework.core.SettingService
import bropen.toolkit.utils.StringUtils
import bropen.toolkit.utils.grails.BeanUtils
import org.elasticsearch.client.Client

/**
 * ES 核心服务
 */
@grails.compiler.GrailsCompileStatic
class EsCoreService {

	SettingService settingService

	private Client client = null

	private static Boolean enabled = null

	/**
	 * 是否启用
	 */
	public synchronized static Boolean enabled() {
		if (enabled == null) {
			EsApiWrapper.getApi()
			String hosts = (BeanUtils.getBean("settingService") as SettingService).get("bropen.search.ElasticSearch.hosts")
			enabled = StringUtils.means(hosts, false)
		}
		return enabled
	}

	/**
	 * 获取 Es 客户端对象
	 *
	 * @param options.reload 是否重新加载客户端
	 */
	public Client getClient(Map options = null) {
		if (client && options?.reload) {
			closeClient()
		}

		if (client == null || options?.reload) {
			String clusterName = settingService.get("bropen.search.ElasticSearch.cluster.name"),
				   username = settingService.get("bropen.search.ElasticSearch.login.username"),
				   password = settingService.get("bropen.search.ElasticSearch.login.password"),
				   hosts = settingService.get("bropen.search.ElasticSearch.hosts")

			List<Map> addresses = []
			try {
				for (String host in hosts.tokenize(";")) {
					addresses << [address: InetAddress.getByName(host.tokenize(":")[0]),
								  port   : host.tokenize(":")[1].toInteger().intValue()]
				}
			} catch (UnknownHostException e) {
				log.error(null, e)
				return null
			}
			Map clientSetting = [clusterName: clusterName,
								 addresses  : addresses,
								 username   : username,
								 password   : password]
			if (log.isTraceEnabled()) log.trace("Getting es client: " + clientSetting.toString())
			client = EsApiWrapper.getClient(clientSetting)
		}

		return client
	}

	/**
	 * 关闭连接
	 */
	public void closeClient() {
		client?.close()
		client = null
	}

	public Integer getQueryPageSize() {
		settingService.get("bropen.search.ElasticSearch.page.query.size") as Integer
	}

	public Integer getImportPageSize() {
		settingService.get("bropen.search.ElasticSearch.page.import.size", 1000) as Integer
	}

	public String getFileParserUrl() {
		settingService.get("bropen.search.url.file.parse") as String
	}


}