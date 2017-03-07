/* Copyright © 2016 北京博瑞开源软件有限公司版权所有。*/
package bropen.search

import bropen.framework.core.Attachment
import bropen.framework.core.AttachmentService
import bropen.framework.core.Locker
import bropen.framework.core.security.DomainApplication
import bropen.search.es.EsCoreService
import bropen.search.es.EsIndexService
import bropen.toolkit.utils.DateUtils
import bropen.toolkit.utils.grails.DbUtils
import bropen.toolkit.utils.network.HttpClient
import static bropen.search.Constants.LASTUPDATED_ATTACHMENT
import static bropen.search.Constants.TYPE_DOCUMENT
import static bropen.search.Constants.TYPE_ATTACHMENT
import static bropen.search.Constants.FIELD_ATTACHMENTS

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * 附件索引服务
 */
@grails.transaction.Transactional(readOnly = true)
class AttachmentIndexService {

	AttachmentService attachmentService

	EsCoreService esCoreService
	EsIndexService esIndexService

	/** 初始化或增量更新时的附件解析线程数量，默认为1 */
	public static Integer PARSE_THREADS_COUNT = 1

	/** 链接附件解析服务器的HTTP设置 */
	public static Map PARSE_HTTP_OPTIONS = [connectionTimeout: 30000, socketTimeout: 60000]

	/**
	 * 初始化附件索引
	 */
	synchronized public void init() {
		if (Locker.lockExit(Constants.INIT_LOCK, false, 24 * 60 * 60 * 1000)) {
			try {
				update([lastUpdated: DateUtils.parse("2010-10-10")])?.join()
			} finally {
				Locker.unlock(Constants.INIT_LOCK, false)
			}
		} else {
			log.warn("Failed to get initial lock, quit.")
		}
	}

	/**
	 * 同步（单线程）更新指定文档下的附件
	 *
	 * @param docClass
	 * @param docIds
	 */
	synchronized public void update(String docClass, List<Long> docIds) {
		indexAttachments("where dataClass='${docClass}' and " + DbUtils.assembleInLisHql("dataId", docIds))
	}

	/**
	 * 同步（单线程）更新指定的附件
	 */
	synchronized public void update(List<Long> attachmentIds) {
		indexAttachments("where " + DbUtils.assembleInLisHql("id", attachmentIds))
	}

	/**
	 * 异步（多线程）更新附件索引
	 *
	 * @param options.lastUpdated 本次需要索引的附件最近更新时间。
	 * 			空则根据上次更新的索引历史自动计算；可以设置一个比较早的日期重新索引所有附件。
	 * @param options.docClasses 附件所属的文档类名列表。
	 * 			空则取当前应用下所有的文档类。
	 */
	synchronized public Thread update(Map options = null) {
		if (options == null) options = [:]
		if (!prepareOptions(options)) return

		// 需要解析内容的附件队列
		final BlockingQueue<List<Map>> queue4Parse = new ArrayBlockingQueue<List<Map>>(10000)
		// 需要索引的附件队列
		final BlockingQueue<Map> queue4Index = new ArrayBlockingQueue<Map>(10000)

		// 解析线程的倒计数器
		Integer parseThreadCount = PARSE_THREADS_COUNT
		final CountDownLatch parseThreadCountdown = new CountDownLatch(parseThreadCount)

		// 查出需要索引的附件
		// 推送到队列 queue4Parse 中
		// 查出要删除的附件直接启动一个线程删除
		Long currentTimeMillis = System.currentTimeMillis()
		DbUtils.startPersistentThread("bro-search-att-query-" + currentTimeMillis) {
			queryAttachments(queue4Parse, options)
		}

		// 从队列 queue4Parse 中获取要解析的附件
		// 解析后将结果推送到 queue4Index 队列中
		for (int i in 1..parseThreadCount) {
			Thread.startDaemon("bro-search-att-parse-${i}-" + currentTimeMillis) {
				try {
					parseAttachments(queue4Parse, queue4Index, parseThreadCountdown)
				} catch (Exception e) {
					parseThreadCountdown.countDown()
					throw e
				}
			}
		}

		// 从队列 queue4Index 中获取要索引的数据
		// 索引到 es 中，并且记录索引成功的记录
		return Thread.startDaemon("bro-search-att-index-" + currentTimeMillis) {
			indexAttachments(queue4Index, parseThreadCountdown)
		}
	}

	private Boolean prepareOptions(Map options) {
		// 如果参数中没有 docClasses，则从本应用中找
		if (!options.docClasses) {
			if (options.lastUpdated) {
				options.docClasses = Attachment.executeQuery(
						"select distinct dataClass from ${Attachment.name} where application=? and lastUpdated > ?",
						[DomainApplication.current(), options.lastUpdated])
			} else {
				options.docClasses = Attachment.executeQuery(
						"select distinct dataClass from ${Attachment.name} where application=?",
						[DomainApplication.current()])
			}
		}
		// 如果参数里没有 lastUpdated，则从本应用的文档索引里找最近更新时间
		if (!options.lastUpdated && options.docClasses) {
			for (String docClass in options.docClasses.toList()) {
				if (esIndexService.isIndexExists(EsIndexService.getIndexName(docClass))) {
					options.put(docClass,
							esIndexService.getLastUpdated(EsIndexService.getIndexName(docClass), LASTUPDATED_ATTACHMENT))
				} else {
					// 索引不存在！
					options.docClasses.remove(docClass)
				}
			}
		}
		// 没有需要更新的附件，直接退出
		return options.docClasses ? true : false
	}

	private static String hql_select_from = """select new map(
				id as id, dataId as dataId, dataClass as dataClass, lastUpdated as lastUpdated, fileSize as fileSize,
				fileName as fileName, savePath as savePath, saveName as saveName) from ${Attachment.name} """

	/**
	 * 查询需要更新索引的附件
	 */
	private void queryAttachments(BlockingQueue<List<Map>> queue4Parse, Map options) {
		String hql = hql_select_from
		StringBuilder whereHql = new StringBuilder("where dataClass=? and deleted=? and lastUpdated > ? ")
		hql += whereHql.toString() + " order by lastUpdated"
		String hqlCount = "select count(*) from ${Attachment.name} ${whereHql}"

		Integer max = esCoreService.getImportPageSize()
		Integer totalUpdated = 0, totalDeleted = 0
		for (String docClass in options.docClasses) {
			if (!esIndexService.isIndexExists(EsIndexService.getIndexName(docClass))) continue
			List<Object> queryParams = [docClass, null, (options.get(docClass) ?: options.lastUpdated)] as List<Object>
			// 更新：
			// 按照 docClass 推送列表到附件解析线程，牺牲并发性能，确保执行顺序正确、最后的 saveLastUpdated 正确
			queryParams[1] = false
			Integer offset = 0,
					total = Attachment.executeQuery(hqlCount, queryParams, [cache: false])[0]
			final List<Map> updated = []
			totalUpdated += total
			while (total > 0 && offset < total) {
				List<Map> list = Attachment.executeQuery(hql, queryParams, [max: max, offset: offset, cache: false])
				offset += max
				for (Map m in list) {
					updated << assembleUpdateParams(m)
				}
				if (offset < total) {
					while (queue4Parse.size() > queue4Parse.remainingCapacity() * 0.7)
						Thread.sleep(5000)
				}
			}
			queue4Parse.put(updated)
		}
		log.info("Found [${totalUpdated}] attachments to update and [${totalDeleted}] to delete.")

		// 往队列里推送一个结束标记，和 parseAttachments 通信
		queue4Parse.put([[finished: true]])
	}

	// 超过 20M 则不解析
	private static Long maxSize = (20 << 20)

	// 下列类型的附件不解析
	private static final List<String> suffixes = [
			"zip", "gz", "rar", "7z", "tar", "iso", "jar", "war",
			"mp3", "mp4", "mpeg", "avi", "mov", "asf", "wmv", "navi", "3gp", "mkv", "flv", "f4v", "rmvb", "ts", "ogg",
			"jpeg", "tiff", "psd", "png", "swf", "svg", "pcx", "dxf", "wmf", "emf", "lic", "eps", "tga", "bmp", "jpg", "gif"]

	private Map assembleUpdateParams(Map att) {
		String filename
		if (att.fileSize > maxSize
				|| (att.fileName.substring(att.fileName.lastIndexOf(".") + 1) as String).toLowerCase() in suffixes) {
			filename = null
		} else {
			String savePathRoot = attachmentService.getSavePathRoot()
			filename = savePathRoot + att.savePath + File.separator + att.saveName
			if (!(new File(filename).exists())) {
				filename = null
			} else {
				filename = att.savePath + File.separator + att.saveName // root已配置在 fileparser 的仓库路径里
			}
		}
		return [lastUpdated: att.lastUpdated,
				filename   : filename,
				// @see esIndexService.indexDocuments
				index      : EsIndexService.getIndexName(att.dataClass),
				type       : TYPE_ATTACHMENT,
				id         : att.id,
				parent     : att.dataId,
				//field      : [FIELD_ATTACHMENTS],
				value      : [id: att.id, name: att.fileName, content: null]]
	}

	/**
	 * 解析附件，提取文本
	 */
	private void parseAttachments(
			BlockingQueue<List<Map>> queue4Parse, BlockingQueue<Map> queue4Index, CountDownLatch parseThreadCountdown) {
		String url = esCoreService.getFileParserUrl()
		Integer totalParsed = 0, totalContent = 0, total = 0
		while (true) {
			if (queue4Index.size() < queue4Index.remainingCapacity() * 0.8) {
				List<Map> list = queue4Parse.poll()
				if (list) {
					if (list.first().finished) {
						// 处理结束，退出线程
						queue4Parse.put(list)
						parseThreadCountdown.countDown()
						break
					} else {
						// 解析相同 docClass 的附件
						Map<String, Integer> result = parseAttachments(list, url, queue4Index)
						totalParsed += result.totalParsed
						totalContent += result.totalContent
						total += list.size()
					}
				}
			}
			Thread.sleep(500)
		}
		if (totalParsed > 0) log.info("Total parsed [${totalContent}/${totalParsed}/${total}] attachments.")
	}

	private Map<String, Integer> parseAttachments(List<Map> list, String url, BlockingQueue<Map> queue4Index) {
		Map<String, Integer> result = [totalContent: 0, totalParsed: 0]
		for (Map m in list) {
			int countdown = 3
			boolean failed = false
			while (!failed || countdown > 0) {
				String filename = m.remove("filename")
				if (filename == null) break
				try {
					//String content = "附件附件附件附件--${m.item.id}--" + m.remove("filename") + "---" + url
					String content = HttpClient.post(url)
							.addParameter("filename", filename)
							.setConnectTimeout(PARSE_HTTP_OPTIONS.connectionTimeout)
							.setSocketTimeout(PARSE_HTTP_OPTIONS.socketTimeout)
							.execute().getString()
					if (content && !content.startsWith("ERROR: ")) {
						m.value.content = content
						queue4Index.put(m)
						result.totalContent++
					}
					result.totalParsed++
					break
				} catch (IOException e) {
					// 请求出错，重复请求 countdown 次
					failed = true
					countdown--
					Thread.sleep(50)
				}
			}
		}
		if (result.totalParsed > 0) log.debug("Parsed [${result.totalContent}/${result.totalParsed}/${list.size()}] attachments.")
		return result
	}

	/**
	 * 索引附件
	 */
	private void indexAttachments(BlockingQueue<Map> queue4Index, CountDownLatch parseThreadCountdown) {
		Integer total = 0
		while (true) {
			List<Map> list = []
			queue4Index.drainTo(list, esCoreService.getImportPageSize())
			if (!list) {
				// 队列里没有数据了，并且 parseThreads 全部结束了，则退出线程
				if (parseThreadCountdown.await(1000, TimeUnit.MILLISECONDS)) {
					queue4Index.drainTo(list, esCoreService.getImportPageSize()) // 必须再来一次！
					if (!list) break
				}
			}

			int countdown = 3
			boolean failed = false
			while (!failed || countdown > 0) {
				try {
					total += list.size()
					esIndexService.indexDocuments(list)
					// 保存最后更新时间
					for (Map.Entry entry in list.groupBy { it.index }) {
						String index = entry.key
						Date lastUpdated = entry.value*.lastUpdated.max()
						esIndexService.saveLastUpdated(index, LASTUPDATED_ATTACHMENT, lastUpdated)
					}
					break
				} catch (Exception e) {
					// 失败了重复提交 countdown 次
					failed = true
					countdown--
				}
			}
		}
		log.info("Indexed [${total}] attachments.")
	}

	/**
	 * 同步索引附件
	 */
	private void indexAttachments(String hqlWhere) {
		List<Map> list = Attachment.executeQuery(hql_select_from + hqlWhere)
		if (list) {
			for (int i = list.size() - 1; i >= 0; i--) {
				list[i] = assembleUpdateParams(list[i])
			}
			BlockingQueue<Map> queue = new ArrayBlockingQueue<Map>(1000)
			String url = esCoreService.getFileParserUrl()
			Map<String, Integer> parseResult = parseAttachments(list, url, queue)
			while (queue.size()) {
				queue.drainTo(list, esCoreService.getImportPageSize())
				esIndexService.indexDocuments(list)
			}
			log.info("Indexed [${parseResult.totalContent}/${parseResult.totalParsed}/${list.size()}] attachments.")
		}
	}

}