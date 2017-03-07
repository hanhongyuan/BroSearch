/* Copyright © 2016-2017 北京博瑞开源软件有限公司版权所有。*/
package bropen.search

import bropen.framework.core.Attachment
import bropen.framework.core.Locker
import bropen.framework.core.security.DomainApplication
import bropen.search.config.Searchable
import bropen.search.config.SearchableService
import bropen.search.es.EsCoreService
import bropen.search.es.EsIndexService
import bropen.search.es.EsSearchService
import bropen.search.orm.Entity
import bropen.search.orm.HibernateEventListener
import bropen.toolkit.utils.CollectionUtils
import bropen.toolkit.utils.DateUtils
import bropen.toolkit.utils.StringUtils
import bropen.toolkit.utils.grails.BeanUtils
import bropen.toolkit.utils.grails.DbUtils

import groovy.sql.Sql
import grails.core.DefaultGrailsApplication

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutionException

import static bropen.search.Constants.FIELD_APPLICATION
import static bropen.search.Constants.LASTUPDATED_DOCUMENT
import static bropen.search.Constants.TYPE_DOCUMENT
import static bropen.search.Constants.TYPE_ATTACHMENT
import static bropen.search.Constants.FIELD_CLASS
import static bropen.search.Constants.FIELD_URL
import static bropen.search.Constants.FIELD_TITLE
import static bropen.search.Constants.FIELD_SUMMARY
import static bropen.search.Constants.FIELD_TITLE_MAXSIZE
import static bropen.search.Constants.FIELD_SUMMARY_MAXSIZE
import static bropen.search.Constants.FIELD_PERMISSIONS
import static bropen.search.Constants.FIELD_PERMISSIONS_DEFAULT
import static bropen.search.Constants.FIELD_CATEGORIES

/**
 * 文档索引服务
 */
@grails.transaction.Transactional(readOnly = true)
class SearchableIndexService {

	DefaultGrailsApplication grailsApplication

	AttachmentIndexService attachmentIndexService

	EsCoreService esCoreService
	EsIndexService esIndexService
	EsSearchService esSearchService
	SearchableService searchableService

	private static final Map<String, Boolean> createdIndexes = [:]
	private static final Map<String, Closure> documentAssemblers = [:]
	private static final Map<String, Closure> categoryAssemblers = [:]
	private static final Map<String, Closure> permissionAssemblers = [:]
	private static final Map<String, Closure> mappingAssemblers = [:]


	@grails.compiler.GrailsCompileStatic
	public void bootStrapInit() {
		addPermissionAssembler("DEFAULT_PERMISSION", { String index, Class docClazz, List<Map> docs, Map domainProperties ->
			if (domainProperties.unclassified) {
				for (Map m in docs) {
					m[FIELD_PERMISSIONS] = FIELD_PERMISSIONS_DEFAULT
				}
			}
		})
	}

	/**
	 * 初始化索引
	 *
	 * @param docClass 要索引的文档 Domain 类名
	 */
	public void init(String docClass = null) {
		if (docClass && !BeanUtils.loadClass(docClass)) return
		if (Locker.lockExit(Constants.INIT_LOCK, false, 5 * 60 * 60 * 1000)) {
			try {
				List<String> docClasses
				if (docClass) {
					docClasses = [docClass]
					deleteIndex(searchableService.getIndexName(docClass))
				} else {
					docClasses = searchableService.getSearchableClazzes()*.docClass
					for (String className in docClasses) {
						deleteIndex(searchableService.getIndexName(className))
					}
				}

				//docClasses.remove("cm.finance.plan.FinancePlan") // 4 test only
				if (docClasses.size() > 20) {
					// 多线程
					List<Thread> threads = []
					final List<Exception> exceptions = []
					final int avg = docClasses.size() / 4
					for (int i = 0; i < 5; i++) {
						final List<String> classes = docClasses.subList(i * avg, [(i + 1) * avg, docClasses.size()].min())
						threads << Thread.start {
							try {
								update([docClasses: classes, lastUpdated: DateUtils.parse("2010-10-10"),
										initial   : true, initialAll: (docClass == null)])
							} catch (Exception e) {
								log.error(null, e)
								exceptions << e
							}
						}
					}
					threads*.join()
					if (exceptions.size()) println "Exception occured, please check the log!"
				} else {
					// 单线程
					update([docClasses: docClasses, lastUpdated: DateUtils.parse("2010-10-10"),
							initial   : true, initialAll: (docClass == null)])
				}
			} finally {
				Locker.unlock(Constants.INIT_LOCK, false)
			}
		} else {
			log.warn("Failed to get initial lock, quit.")
		}
	}

	/**
	 * 更新索引
	 *
	 * @param options.docClasses 要索引的文档 Domain 类名列表，空则自动计算
	 * @param options.excludeClasses 从 docClasses 中排除不索引的文档 Domain 类名列表
	 * @param options.lastUpdated 上次索引更新时间，空则根据 docClasses 的历史索引记录自动计算
	 */
	public void update(Map options = null) {
		if (options == null) options = [:]
		options.remove("docIds")
		if (options.docClasses) {
			for (String docClass in options.docClasses?.toList()) {
				if (!BeanUtils.loadClass(docClass)) {
					options.docClasses.remove(docClass)
					log.warn("Class ${docClass} is not exists!")
				}
			}
		} else {
			options.docClasses = searchableService.getSearchableClazzes()*.docClass
		}
		if (options?.excludeClasses && options.docClasses) {
			options.docClasses.removeAll(options.remove("excludeClasses"))
		}
		if (options.docClasses) {
			for (String docClass in options.docClasses) {
				options.put(docClass, options.lastUpdated ?:
						esIndexService.getLastUpdated(searchableService.getIndexName(docClass), TYPE_DOCUMENT))
			}
			options.lastUpdated = null
		} else {
			// 没有可更新的，直接退出
			return
		}

		indexDocuments(options)
	}

	/**
	 * 更新指定文档的索引
	 */
	public void update(String docClass, List<Long> docIds) {
		if (!BeanUtils.loadClass(docClass)) return
		indexDocuments([docClass: docClass, docIds: docIds])
	}

	/**
	 * 更新指定文档的索引
	 */
	public void update(String docClass, Long docId) {
		if (!BeanUtils.loadClass(docClass)) return
		indexDocuments([docClass: docClass, docIds: [docId]])
	}

	/**
	 * 根据注解 Searchable 获取所有需要索引的domain类
	 */
	/*public List<String> getSearchableClazzes() {
		List<String> classes = []
		for (GrailsClass grailsClass in grailsApplication.getArtefacts(DomainClassArtefactHandler.TYPE)) {
			Searchable searchable = grailsClass.clazz.getAnnotation(Searchable)
			if (searchable != null) {
				if (searchable.disabled()) continue
				classes << grailsClass.clazz.name
			}
		}
		return classes
	}*/

	/**
	 * 添加一个闭包，用于拼接额外的文档属性
	 *
	 * @param name 闭包名称，相同则覆盖
	 * @param assembler 用于拼接额外的文档属性的闭包，可接收参数 (String index, Class docClazz, List<Map> docs, Map domainProperties)，
	 * 		其中 docs 为已拼接的文档对象列表，如需增删额外的属性，只需要直接修改每一个文档 Map；
	 * 		domainProperties 为 docClazz 类解析后的Map。
	 */
	@grails.compiler.GrailsCompileStatic
	public void addDocumentAssembler(String name, Closure assembler) {
		documentAssemblers.put(name, assembler)
	}

	/**
	 * 添加一个闭包，用于拼接文档的权限属性
	 *
	 * @param name 闭包名称，相同则覆盖
	 * @param assembler 用于拼接文档权限的闭包，可接收参数 (String index, Class docClazz, List<Map> docs, Map domainProperties)，
	 * 		其中 docs 为已拼接的文档对象列表，只需要直接修改每一个文档 Map 的 bropen.search.Constants.FIELD_PERMISSIONS 值，
	 * 		值的格式为 [e1, o2, ..] 列表，即列表的每一个元素为“标识字符+ID”，标识符包括"u用户、e员工、ei身份、o机构、p岗位、g群组、r角色"；
	 * 		domainProperties 为 docClazz 类解析后的Map。
	 */
	@grails.compiler.GrailsCompileStatic
	public void addPermissionAssembler(String name, Closure assembler) {
		permissionAssemblers.put(name, assembler)
	}

	/**
	 * 添加一个闭包，用于拼接文档的分类属性
	 *
	 * @param name 闭包名称，相同则覆盖
	 * @param assembler 用于拼接文档分类的闭包，可接收参数 (String index, Class docClazz, Map doc, Map domainProperties)，
	 * 		其中 doc 为文档对象，domainProperties 为 docClazz 类解析后的Map。
	 * 		注闭包中仅需返回一条数据分类计算结果即可。
	 */
	@grails.compiler.GrailsCompileStatic
	public void addCategoryAssembler(String name, Closure assembler) {
		categoryAssemblers.put(name, assembler)
	}

	/**
	 * 添加一个闭包，用于创建索引时，生成额外的type mapping。
	 *
	 * @param name 闭包名称，相同则覆盖
	 * @param closure 生成闭包，接收参数 (String index, Class docClazz)，
	 */
	@grails.compiler.GrailsCompileStatic
	public void addMappingAssembler(String name, Closure closure) {
		mappingAssemblers.put(name, closure)
	}

	/**
	 * 添加一个闭包，用于实时监听文档相关的其他domain类实例的变更
	 *
	 * <p>例如：{@code
	 * addDocumentListener ( ProcessInstance.class , { ProcessInstance instance - >
	 * return [class : Class.forName (instance.dataClass) , id: instance.dataId]
	 *})}</p>
	 *
	 * @param clazz 需要监听的类
	 * @param closure 用于生成文档class和id的闭包，接收参数（class 实例），返回map：[class: 文档class, id: 文档id]
	 */
	@grails.compiler.GrailsCompileStatic
	public void addDocumentListener(Class clazz, Closure closure) {
		HibernateEventListener.addDocumentListener(clazz, closure)
	}

	/**
	 * 创建索引
	 * @param index 索引名称
	 */
	@grails.compiler.GrailsCompileStatic
	private void createIndex(Class clazz, String index) {
		if (!createdIndexes.get(index)) {
			esIndexService.createIndexIfNotExists(index, [defaultMapping: true])
			//esIndexService.putMappingIfNotExists(index, TYPE_PERMISSION, TYPE_PROCESS)
			//esIndexService.putMappingIfNotExists(index, TYPE_PROCESS, TYPE_DOCUMENT)
			if (mappingAssemblers) {
				for (Closure c in mappingAssemblers.values())
					c.call(index, clazz)
			}
			esIndexService.putMappingIfNotExists(index, TYPE_ATTACHMENT, TYPE_DOCUMENT)
			esIndexService.putMappingIfNotExists(index, TYPE_DOCUMENT)
			createdIndexes.put(index, true)
		}
	}

	/**
	 * 删除索引
	 * @param index
	 */
	@grails.compiler.GrailsCompileStatic
	public void deleteIndex(String index) {
		createdIndexes.remove(index)
		esIndexService.deleteIndex(index)
	}

	/**
	 * 索引文档
	 *
	 * 组合1：
	 * @param options.docClasses
	 * @param options.[docClass..] 每个 docClass 对应的最近更新时间
	 * @param initial 是否时初始化索引
	 *
	 * 组合2：
	 * @param options.docClass
	 * @param options.docIds
	 *
	 * @throws InterruptedException
	 * @throws java.util.concurrent.ExecutionException
	 */
	private void indexDocuments(Map<String, Object> options) throws InterruptedException, ExecutionException {
		if (options.docIds) {
			Integer total = indexDocuments(options.docClass, [docIds: options.docIds])
			if (total) attachmentIndexService.update(options.docClass, options.docIds)
			log.info("Indexed [${total}] documents: " + options.docClass)
		} else {
			for (String docClass in options.docClasses) {
				Date now = new Date(), lastUpdated = options.get(docClass)
				Integer total = indexDocuments(docClass, [lastUpdated: lastUpdated, initial: options.initial])
				if (total && !options.initialAll)
					attachmentIndexService.update([docClasses: [docClass], lastUpdated: lastUpdated])
				log.info("Indexed [${total}] [${docClass}] documents since [${DateUtils.formatDatetime(lastUpdated)}].")
				// 保存最后更新时间
				esIndexService.saveLastUpdated(searchableService.getIndexName(docClass), LASTUPDATED_DOCUMENT, now)
			}
		}
	}

	/**
	 * @param options.docIds 或 lastUpdated + initial
	 */
	private Integer indexDocuments(String docClass, Map<String, Object> options) {
		if (options.initial) log.info("Start to init document indices: " + docClass)
		Class clazz = Class.forName(docClass)
		initDocumentIndex(clazz)
		Integer total = doIndexDocuments(clazz, options)
		//indexProcessInstances(docClass, options)    // 导入二级
		//indexProcessPermissions(docClass, options)  // 导入三级
		return total
	}

	private void initDocumentIndex(Class clazz) throws InterruptedException, ExecutionException {
		String index = searchableService.getIndexName(clazz)
		if (esIndexService.isIndexExists(index)) {
			// 索引存在，并且没有数据，删除索引
			Page p = esSearchService.search([index: index, max: 1])
			if (p.total == 0) deleteIndex(index)
		}
		createIndex(clazz, index)
	}

	/**
	 * @param options.docIds 或 lastUpdated + initial
	 */
	private Integer doIndexDocuments(Class docClazz, Map options) {
		Integer max = esCoreService.getImportPageSize()
		String index = searchableService.getIndexName(docClazz)

		Searchable searchable = Searchable.findByApplicationAndDocClass(DomainApplication.current(), docClazz.name)
		Map<String, Map> clazzProps = searchableService.getDomainProperties(docClazz, searchable)
		Map properties = clazzProps[docClazz.name]
		List<Object> queryParams = [] as List<Object>
		String selectHql = assembleSelectHql(properties),
			   whereHql = ""
		if (options.lastUpdated) {
			if (BeanUtils.isPersistentProperty(docClazz, "lastUpdated")) {
				whereHql = " where d.lastUpdated > ?"
				queryParams << options.lastUpdated
			} else {
				log.warn("Domain 类 ${docClazz.name} 没有时间戳属性 lastUpdated，无法增量更新索引！")
			}
		} else if (options.docIds) {
			whereHql = DbUtils.assembleInLisHql("d.id", (Collection) options.docIds, [prefix: " where "])
		} else {
			throw new IllegalArgumentException("No options.lastUpdated and options.docIds !!")
		}

		Integer offset = 0
		Integer total = docClazz.executeQuery("select count(*) from ${docClazz.name} d ${whereHql}", queryParams, [cache: false])[0]

		String hql = selectHql + whereHql
		Map indexOptions = options.initial ? [overwrite: true] : null
		while (total > 0 && offset < total) {
			List<Map> docs = assembleDocuments(docClazz, clazzProps, hql, queryParams, max, offset)
			if (docs.size() != 0) {
				offset += max
				assembleSearchableProps(index, docClazz, docs, properties)
				esIndexService.indexDocuments(index, TYPE_DOCUMENT, docs, indexOptions)
			}
		}
		return total
	}

	/**
	 * 拼接附加属性：标题、摘要、url、permission、documentAssemblers闭包中的属性 等
	 */
	@grails.compiler.GrailsCompileStatic
	private void assembleSearchableProps(String index, Class docClazz, List<Map> docs, Map properties) {
		// 文档、权限
		for (Closure closure in documentAssemblers.values()) {
			closure.call(index, docClazz, docs, properties)
		}
		for (Closure closure in permissionAssemblers.values()) {
			closure.call(index, docClazz, docs, properties)
		}
		// 标题等
		// 注：先解析文档，再解析标题等，方便‘标题’、‘备注’引用文档中的某些属性，如：processInstance.title
		for (Map doc in docs) {
			doc[FIELD_CLASS] = docClazz.name
			doc[FIELD_URL] = doc[FIELD_URL] ?: ((String) properties.url).replace("{id}", doc.id.toString())
			doc[FIELD_TITLE] = doc[FIELD_TITLE] ?: assembleSearchableProps(doc, (String[]) properties.title, FIELD_TITLE_MAXSIZE)
			doc[FIELD_SUMMARY] = doc[FIELD_SUMMARY] ?: assembleSearchableProps(doc, (String[]) properties.summary, FIELD_SUMMARY_MAXSIZE)
		}
		// 当前class所在的分类及分类下所有分类
		List<Map> categories = Searchable.getAllCategories([clazz: docClazz])
		if (categories) {
			for (Map doc in docs) {
				doc[FIELD_CATEGORIES] = [:]
				for (Map category in categories) {
					String code = category.code as String
					if (categoryAssemblers.containsKey(code))
						doc[FIELD_CATEGORIES][code] = categoryAssemblers[code].call(index, docClazz, doc, properties)
				}
			}
		}
	}

	/**
	 * 解析附加属性：标题、摘要
	 * @return
	 */
	@grails.compiler.GrailsCompileStatic
	private String assembleSearchableProps(Map doc, String[] templates, Integer maxSize) {
		if (!templates) return null
		StringBuilder sb = new StringBuilder()
		for (String t in templates) {
			if (t.contains('$')) {
				sb << StringUtils.template(t, doc)
			} else if (t.contains(".")) {
				String[] arr = t.split("\\.")
				Map m = doc
				for (int i = 0; i < arr.size() - 1; i++) {
					m = (Map) m[arr[i]]
					if (!m) break
				}
				if (m) sb << m[arr[arr.size() - 1]]
			} else {
				sb << doc[t]
			}
			sb << " "
		}
		return (sb.length() > maxSize ? "${sb.substring(0, maxSize - 3)}..." : sb.toString())
	}

	@grails.compiler.GrailsCompileStatic
	private List<Map> assembleDocuments(Class docClazz, Map<String, Map> clazzProps, String hql, List queryParams, Integer max, Integer offset) {
		List<Map> docs = docClazz.invokeMethod("executeQuery",
				[hql, queryParams, [max: max, offset: offset, fetchSize: max, cache: false]] as Object[]) as List
		if (docs.size() > 0) {
			Long appId = DomainApplication.current().id
			Map cache = [main: [:]]
			for (Map doc in docs) {
				cache.main[doc.id] = doc
				// 拼应用字段
				doc.put(FIELD_APPLICATION, appId)
			}
			Map props = clazzProps[docClazz.name]
			for (Map prop in (props.children as List<Map>)) {
				if (1 == (Integer) prop.child)
					assembleRefDocuments(docClazz, prop, clazzProps, cache, "main", (String) prop.name, 1)
			}
			//assembleMappingFields(docs, props)
			assembleMappingSqlFields(docClazz, docs, props)
		}
		DbUtils.getCurrentSession().clear()
		return docs
	}

	@grails.compiler.GrailsCompileStatic
	private void assembleRefDocuments(Class mainDocClazz, Map prop, Map<String, Map> clazzProps,
									  Map cache, String mainCacheKey, String fieldPath, Integer level) {
		Class clazz = prop.reftype
		Map props = clazzProps[fieldPath]
		Boolean isCollection = Collection.class.isAssignableFrom(prop.type as Class)

		Map<Long, Map> mainDocs = cache[mainCacheKey] as Map<Long, Map>
		String hql = assembleSelectHql(props)

		String refField, // 子类中从属于主类的属性名称
			   childFieldDataId // 主类数据中 子类类型属性名称Id
		if (isCollection) {
			// 主类:子类 = 1:多 计算子类中从属于主类的属性名称，如果不存在，则退出
			if (props.parent) {
				// eg.: belongsTo = [foobar: Foobar] 或 belonsTo = Foobar
				for (Map m in (props.parent as List<Map>)) {
					if (mainDocClazz == (Class) m.type) {
						refField = (String) m.name
						break
					}
				}
			}
			if (!refField) {
				// eg.: Foobar foobar
				for (Map.Entry<String, Map> entry in BeanUtils.getPersistentProperties(clazz).entrySet()) {
					if (!Collection.class.isAssignableFrom(entry.value.type as Class) && mainDocClazz == entry.value.reftype)
						refField = entry.key
				}
			}
			if (!refField) return

			Set mainDocIds = mainDocs.keySet()
			hql += DbUtils.assembleInLisHql("d.${refField}.id", mainDocIds, [prefix: " where "])
		} else {
			// 主类:子类 = 1:1 计算主类中 子类类型属性 名称，如果不存在，则退出
			childFieldDataId = "${prop.name}Id"
			List<Long> ids = []
			for (Map.Entry<Long, Map> e in mainDocs.entrySet()) {
				if (e.value[childFieldDataId]) {
					ids << (Long) e.value[childFieldDataId]
				}
			}
			// 没有数据
			if (!ids) return
			hql += DbUtils.assembleInLisHql("d.id", ids, [prefix: " where "])
		}

		List<Map> docs = clazz.invokeMethod("executeQuery",
				[hql, [fetchSize: esCoreService.getImportPageSize(), cache: false]] as Object[]) as List

		if (docs.size() > 0) {
			mainCacheKey = "${mainDocClazz}.${prop.name}"
			Map map = cache[mainCacheKey] = [:]
			if (isCollection) {
				for (Map doc in docs) {
					map[doc.id] = doc
					// 主文档中添加对子文档的引用
					Map mainDoc = mainDocs[doc[refField + "Id"]] as Map
					if (mainDoc) {
						if (isCollection) {
							if (mainDoc[prop.name] == null)
								mainDoc[prop.name] = []
							(mainDoc[prop.name] as List) << doc
						} else {
							mainDoc[prop.name] = doc
						}
					}
				}
			} else {
				for (Map doc in docs) map[doc.id] = doc
				for (Map.Entry<Long, Map> e in mainDocs.entrySet()) {
					if (e.value[childFieldDataId]) {
						Long subId = (Long) e.value[childFieldDataId]
						if (map.containsKey(subId)) {
							e.value[prop.name] = map[subId]
							e.value.remove(childFieldDataId)
						}
					}
				}
			}
			DbUtils.getCurrentSession().clear()
			// 解析子文档
			for (Map p in (props.children as List<Map>)) {
				assembleRefDocuments(clazz, p, clazzProps, cache, mainCacheKey, fieldPath + "." + p.name, level + 1)
			}
			// 解析映射字段
			//assembleMappingFields(docs, props)
			assembleMappingSqlFields(clazz, docs, props)
		}
	}

	/**
	 * 拼接注解 @SearchableField 里的 mappingSql
	 */
	@grails.compiler.GrailsCompileStatic
	private void assembleMappingSqlFields(Class clazz, List<Map> docs, Map props) {
		if (!props.mappingSqls) return
		Sql sqlInst = DbUtils.getCurrentSql(DbUtils.getDataSourceNameByDomain(clazz.name))
		try {
			BeanUtils.setProperty(sqlInst, "fetchSize", esCoreService.getImportPageSize())
			for (Map p in ((List<Map>) props.mappingSqls)) {
				String sql = p.mappingSql
				String idField = sql.tokenize(" ")[1]
				sql += (sql.contains("where") ? " " : " where ") + DbUtils.assembleInLisHql(idField, docs*.id)
				Map<Long, List<Map>> rows = sqlInst.rows(sql).groupBy { it.ID.toString() } as Map
				for (Map doc in docs) {
					// 注：必须 toString，否则类型可能不一致（BigDecimal、Long 等）
					if (rows.containsKey(doc.id.toString()))
						doc.put(p.name, rows.get(doc.id.toString())*.CONTENT)
				}
			}
		} finally {
			sqlInst?.close()
		}
	}

	/**
	 * 关联的特殊属性处理
	 * <p>如 Emplyee 属性 emp，转换成 {emp: {id: xxx, name: xxx}} 的简单对象。</p>
	 */
	/*@grails.compiler.GrailsCompileStatic
	private void assembleMappingFields(List<Map> docs, Map props) {
		List<Map> attrs = [], mappings = []
		if (props.ordinaries) attrs.addAll(props.ordinaries as List<Map>)
		if (props.others) attrs.addAll(props.others as List<Map>)
		if (props.parent) attrs.addAll(props.parent as List<Map>)
		for (Map p in attrs) {
			if (p.nullable && p.mapping)
				mappings << p
		}

		Map<String, Map<Long, List<Map>>> cache = [:] // Map<属性名 : Map<属性数据Id值 : [文档列表]>>
		for (Map doc in docs) {
			for (Map p in mappings) {
				String name = p.name as String
				String nameId = name + "Id"
				if (doc[nameId] == null) {
					doc.remove(nameId)
				} else {
					if (!cache.containsKey(name)) cache[name] = [:]
					if (!cache[name].containsKey(doc[nameId])) cache[name][doc[nameId] as Long] = []
					cache[name][doc.remove(nameId) as Long] << doc
				}
			}
		}

		// 一个文档可能含有多个Employee、Organization属性，组装后同一个类型属性一次hql全部查询
		Map<Class, Map<String, String[]>> fields = [:] // Map<clazzName, [属性名:mapping]>
		for (Map p in mappings) {
			Class clazz = (Class) p.type
			if (!fields.containsKey(clazz)) fields[clazz] = [:]
			((Map) fields[clazz]).put((String) p.name, (String[]) p.mapping)
		}

		for (Map.Entry<Class, Map<String, String[]>> e in fields.entrySet()) {
			assembleMappingFields(cache, e.key, e.value)
		}
	}

	@grails.compiler.GrailsCompileStatic
	private void assembleMappingFields(Map<String, Map<Long, List<Map>>> cache, Class propClazz, Map<String, String[]> prop) {
		Set propIds = new HashSet()
		for (String field in prop.keySet()) {
			if (cache.containsKey(field))
				propIds.addAll(((Map) cache[field]).keySet())
		}
		if (!propIds) return

		// 不同属性需要查询的子属性可能不同
		Map<String, Map<String, String>> mappings = [:] // Map<属性名, Map<子类查询的别名， 索引中的字段名>>
		Map<String, String> customProps = [:]
		for (Map.Entry<String, String[]> p in prop.entrySet()) {
			mappings[p.key] = [:]
			for (String m in p.value) {
				String[] arr = m.split("@")
				String alias = arr[1].replaceAll("\\.", "_")
				if (!customProps.containsKey(arr[1])) {
					customProps[arr[1]] = alias
				}
				mappings[p.key].put(alias, arr[0])
			}
		}
		String hql = assembleSelectHql([clazz: propClazz], [customProps: customProps] as Map)
		List<Map> list = propClazz.invokeMethod("executeQuery",
				[hql + DbUtils.assembleInLisHql("d.id", propIds, [prefix: " where "]),
				 [fetchSize: esCoreService.getImportPageSize(), cache: false]] as Object[]) as List
		Map<Long, Map> objects = [:]
		for (Map obj in list) objects[(Long) obj.id] = obj // 注：开发者写的mapping中，必须含有小写"id"用于对应

		for (Map.Entry<String, Map<String, String>> e in mappings.entrySet()) {
			if (!cache.containsKey(e.key)) continue
			for (Map.Entry<Long, List<Map>> docEntry in cache[e.key].entrySet()) {
				for (Map doc in docEntry.value) {
					Map obj = objects[docEntry.key]
					for (Map.Entry<String, String> ee in e.value.entrySet()) {
						doc[ee.value] = obj[ee.key]
					}
				}
			}
		}
	}*/

	/**
	 * 拼接没有 where 的 hql 前缀：select new map(...) from xxx d
	 *
	 * @param props
	 * @param options.customProps 自定义查询字段及查询别名，类型为 {@code Map < String , String >}
	 * @param options.excludeProps 不查询的字段名称列表，类型为 {@code List < String >}
	 */
	@grails.compiler.GrailsCompileStatic
	private String assembleSelectHql(Map props, Map<String, Object> options = null) {
		StringBuilder hql = new StringBuilder("select new map(")
		boolean b = false

		if (props.ordinaries) {
			for (Map p in (props.ordinaries as List<Map>)) {
				if (!options?.excludeProps || !(p.name in options.excludeProps)) {
					if (b) hql << "," else b = true
					hql << "d." << p.name << " as " << p.name
				}
			}
		}

		if (props.parent) {
			for (Map p in (props.parent as List<Map>)) {
				if (!options?.excludeProps || !(p.name in options.excludeProps)) {
					if (b) hql << "," else b = true
					hql << "d." << p.name << ".id as " << p.name << "Id"
				}
			}
		}

		if (options?.customProps instanceof Map) {
			for (Map.Entry e in (options.customProps as Map).entrySet()) {
				if (b) hql << "," else b = true
				hql << "d." << e.value << " as " << e.key
			}
		}

		Map<String, String> mappingOuterJoinAlias = [:] // Map<属性名, 别名>
		if (props.mappings) {
			for (Map p in (props.mappings as List<Map>)) {
				String[] mapping = (String[]) p.mappingProps
				if (p.nullable) {
					String alias = "_${p.name}_"
					mappingOuterJoinAlias.put((String) p.name, alias)
					for (String str in mapping) {
						if (b) hql << "," else b = true
						String[] arr = str.split("@")
						hql << alias << "." << arr[1] << " as " << arr[0]
					}
				} else {
					for (String str in mapping) {
						if (b) hql << "," else b = true
						String[] arr = str.split("@")
						hql << "d." << p.name << "." << arr[1] << " as " << arr[0]
					}
				}
			}
		}
		// 非集合子类，查询 id
		if (props.children) {
			for (Map p in (props.children as List<Map>)) {
				Boolean isCollection = Collection.class.isAssignableFrom(p.type as Class)
				if (!isCollection) {
					if (b) hql << "," else b = true
					if (p.nullable) {
						String alias = "_${p.name}_"
						mappingOuterJoinAlias.put((String) p.name, alias)
						hql << alias << ".id as " << p.name << "Id"
					} else {
						hql << "d." << p.name << ".id as " << p.name << "Id"
					}
				}
			}
		}

		hql << ") from " << (props.clazz as Class).name << " d "
		if (mappingOuterJoinAlias) {
			for (Map.Entry<String, String> e in mappingOuterJoinAlias.entrySet())
				hql << " left outer join d." << e.key << " " << e.value
		}
		return hql.toString()
	}

	/**
	 * 索引流程实例（第二级数据）
	 *
	 * @param docClass
	 * @param options.lastUpdated
	 * @param options.docIds
	 */
	/*private void indexProcessInstances(String docClass, Map options) {
		doIndexProcess(docClass, options, ProcessInstance.class, TYPE_PROCESS,
				{ Map pi ->
					return searchableService.getIndexName(pi.dataClass)
				},
				{ Map pi ->
					return pi.dataId
				},
				null)
	}

	private void indexProcessInstance(String dataClass, Long docId) {
		indexProcessInstances(dataClass, [docIds: [docId]])
	}*/

	/**
	 * 索引流程实例的权限（第三级数据）
	 *
	 * @param options.lastUpdated
	 * @param options.docIds
	 */
	/*private void indexProcessPermissions(String docClass, Map options) {
		options.excludeProps = ["id"]
		doIndexProcess(docClass, options, ProcessPermission.class, TYPE_PERMISSION,
				{ Map pp ->
					return searchableService.getIndexName(pp.dataClass)
				},
				{ Map pp ->
					return pp.processInstId
				},
				{ Map pp ->
					String id = "${pp.processInstId}_${pp.uuid}"
					if (pp instanceof Map || pp.id instanceof String) pp.id = id
					return id
				})
	}//*/

	/**
	 *
	 * @param docClass 文档的 Domain 类名
	 * @param options.lastUpdated 或 docIds
	 * @param options.excludeProps
	 * @param options.customProps
	 *
	 * @param clazz 索引的数据类，这里为 ProcessInstance 或 ProcessPermission
	 * @param type 索引的文档类型
	 *
	 * @param indexGetter
	 * @param parentGetter
	 * @param idGetter
	 * @param excludeProps 不索引的属性名
	 */
	/*private void doIndexProcess(String docClass, Map options, Class clazz, String type,
								Closure indexGetter, Closure parentGetter, Closure idGetter) {
		Map props = getDomainProperties(clazz)
		String selectHql = assembleSelectHql(props, options)
		String whereHql = "where d.dataClass=? "
		List<Object> queryParams = [docClass] as List<Object>
		if (options.lastUpdated) {
			if (BeanUtils.isPersistentProperty(clazz, "lastUpdated")) {
				whereHql += " and d.lastUpdated > ?"
				queryParams << options.lastUpdated
			}
		} else if (options.docIds) {
			whereHql += " and " + DbUtils.assembleInLisHql("d.dataId", options.docIds as List)
		}

		Integer offset = 0
		Integer max = esCoreService.getImportPageSize()
		Integer total = clazz.executeQuery(
				"select count(*) from ${clazz.name} d ${whereHql}", queryParams, [cache: false])[0]

		String hql = selectHql + whereHql
		while (total > 0 && offset < total) {
			List<Map> docs = assembleDocuments(clazz, props, hql, queryParams, max, offset)
			if (docs.size() == 0) break
			offset += max
			esIndexService.indexDocuments(indexGetter, type, docs, [parent: parentGetter, idGetter: idGetter])
			DbUtils.getCurrentSession().clear()
		}
	}*/

	/**
	 * 解析某个domain类里的不同属性，以便于拼接查询hql
	 *
	 * @return Map[parent: 父文档属性, ordinaries: 普通属性, children: 动态表属性, mappingSql: 含有mappingSql属性, mappings: 含有mapping的属性]
	 */
	/*@grails.compiler.GrailsCompileStatic
	private Map getDomainProperties(Class clazz, Map<String, Map> fields = null, List path = null) {
		Searchable searchable = Searchable.findByApplicationAndDocClass(DomainApplication.current(), clazz.name)
		return searchableService.getDomainProperties(clazz, searchable)*//*

		*//*Map result = [clazz : clazz, ordinaries: [], children: [], siblings: [],
					  parent: null, mappingSqls: [], mappings: []]
		Map<String, Map> properties = BeanUtils.getPersistentProperties(clazz)
		String prefix = path ? (path.join(".") + ".") : ""
		Set<String> excludes = []
		if (fields) {
			for (Map.Entry<String, Map> e in fields.entrySet()) {
				if (!e.value.disabled && ((String) e.key).startsWith(prefix)) {
					String name = ((String) e.key).substring(prefix.length())
					if (name && !name.contains('.')) {
						excludes << name
						if (e.value.mappingSql)
							((List<Map>) result.mappingSqls) << (properties[e.key] + e.value + [name: name])
						else if (e.value.mapping)
							((List<Map>) result.mappings) << (properties[e.key] + e.value + [name: name])
						else
							((List<Map>) result.siblings) << (properties[e.key] + e.value + [name: name])
					}
				}
			}
		}

		Map<String, Class> belongsTo = BeanUtils.getBelongsTo(clazz)
		// eg.: belongsTo = [foobar: Foobar] 或 belonsTo = Foobar 或 belongsTo = [Foo, Bar]
		String clazzPackage = clazz.name.substring(0, clazz.name.lastIndexOf("."))
		for (Map.Entry<String, Map> prop in properties.entrySet()) {
			String name = prop.key
			if (!belongsTo.containsKey(name) && !excludes.contains(name)) {
				SearchableField sfield = getSearchableField(clazz, prop.value.name as String)
				if (sfield?.exclude()) continue
				Map p = (sfield ? [mapping: sfield.mapping(), mappingSql: sfield.mappingSql()] : [:])
				p.putAll(prop.value)
				if (p.mappingSql) {
					(result.mappingSqls as List) << p
				} else if (p.mapping) {
					(result.mappings as List) << p
				} else if (p.isDomain) {
					Class reftype = p.reftype as Class
					Boolean samePackage = (reftype.name.substring(0, reftype.name.lastIndexOf(".")) == clazzPackage)
					if (Collection.class.isAssignableFrom(p.type as Class)) {
						// 子文档domain：索引所有数据，但是必须有反向关联到父文档的属性，否则应使用注解属性 mappingSql
						(result.children as List) << p
					} else if (!p.mapping && (sfield?.sibling() == 1 || (samePackage && (!sfield || sfield.sibling() == -1)))) {
						// 兄弟文档domain：索引所有数据，但是必须有反向关联到父文档的属性，否则应使用注解属性 mappingSql
						(result.siblings as List) << p
					} else {
						// 关联文档domain（如不同包下的）：仅索引ID 或 mappingXxx 数据
						if (!p.mapping) {
							if (p.type == Employee.class) {
								p.mapping = []
								(p.mapping as List<String>).add(name + "Id@id")
								(p.mapping as List<String>).add(name + "Name@name")
								(result.mappings as List) << p
							} else if (p.type == Organization.class) {
								p.mapping = []
								(p.mapping as List<String>).add(name + "Id@id")
								(p.mapping as List<String>).add(name + "FullName@fullName")
								(result.mappings as List) << p
							}
						}
					}
				} else {
					(result.ordinaries as List) << p
				}
			}
		}
		return result*//*
	}*/

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private static BlockingQueue<Entity> eventQueue = new ArrayBlockingQueue<Entity>(10000)
	private static Thread eventThread = null

	/**
	 * 实时更新文档
	 * @param entity
	 */
	public void update(Entity entity) {
		synchronized (this.class) {
			if (eventThread == null || !eventThread.alive)
				eventThread = startEventListner(eventQueue)
		}
		if (esCoreService.enabled())
			eventQueue.put(entity)
	}

	private Thread startEventListner(final BlockingQueue<Entity> eventQueue) {
		return DbUtils.startPersistentThread("bro-search-searchableindex-listner", 500l, {
			if (esCoreService.enabled()) {
				if (Locker.isLocked(Constants.INIT_LOCK, false)) {
					Thread.sleep(5000)
				} else if (eventQueue.size() > 0) {
					try {
						List<Entity> entities = []
						eventQueue.drainTo(entities, esCoreService.getImportPageSize())
						entities = CollectionUtils.sort(entities, 'desc', 'timestamp')
						log.trace(">> going to update [${entities.size()}] entities.")
						Map<Integer, List<Entity>> entitiesMap = CollectionUtils.groupBy(entities, "type")
						Map<Integer, Set<String>> cache = [:] // 用于查重的缓存，保存各个 bean.class - bean.id
						// 同类型去重
						for (Map.Entry e in entitiesMap) {
							Integer type = e.key
							entities = entitiesMap[type]
							if (!cache.containsKey(type)) cache[type] = ([] as Set<String>)
							for (int i = 0; i < entities.size(); i++) {
								Entity entity = entities[i]
								String key = "${entity.bean.class.name}-${entity.bean.id}"
								if (cache[type].contains(key)) {
									entities.remove(i--) // 只留下最新的
								} else {
									cache[type] << key
								}
							}
						}
						// document、attachment 之间去重
						entities = entitiesMap[Entity.TYPE_ATTACHMENT]
						if (entities) {
							for (int i = entities.size() - 1; i >= 0; i--) {
								Attachment att = (Attachment) entities[i].bean
								String docKey = "${att.dataClass}-${att.dataId}"
								// 文档不在队列里；或者新建文档未保存时，上传附件的dataClass不是类全路径
								if (cache[Entity.TYPE_DOCUMENT]?.contains(docKey) ||
										!BeanUtils.loadClass(att.dataClass)) {
									entities.remove(i)
								}
							}
						}

						// document
						entities = entitiesMap[Entity.TYPE_DOCUMENT]
						if (entities) {
							Map docMap = CollectionUtils.groupBy(entities, "action")
							for (Map.Entry e in docMap[Entity.ACTION_DELETE]?.groupBy { it.bean.class }) {
								esIndexService.deleteDocuments(searchableService.getIndexName(e.key as Class), TYPE_DOCUMENT, e.value*.bean*.id)
							}
							for (Map.Entry e in docMap[Entity.ACTION_UPDATE]?.groupBy { it.bean.class }) {
								update(e.key.name as String, e.value*.bean*.id as List<Long>)
							}
							for (Map.Entry e in docMap[Entity.ACTION_CREATE]?.groupBy { it.bean.class }) {
								update(e.key.name as String, e.value*.bean*.id as List<Long>)
							}
							log.trace("updated [${entities.size()}] documents.")
						}

						// attachmennt
						entities = entitiesMap[Entity.TYPE_ATTACHMENT]
						if (entities) {
							Map attsMap = entities.groupBy { it.bean.deleted }
							if (attsMap.containsKey(Boolean.TRUE)) {
								List<Map> deleteAtts = []
								for (Entity entity in attsMap[true]) {
									Attachment att = (Attachment) entity.bean
									deleteAtts << [index : searchableService.getIndexName(att.dataClass),
												   type  : TYPE_ATTACHMENT,
												   id    : att.id,
												   parent: att.dataId]
								}
								esIndexService.deleteDocuments(deleteAtts)
							}
							if (attsMap.containsKey(Boolean.FALSE)) {
								attachmentIndexService.update(attsMap[false]*.bean*.id)
							}
							log.trace("updated [${entities.size()}] attachments.")
						}
						log.trace("<< entities updated.")
					} catch (Exception e) {
						log.error(null, e)
					}
				}
			}
		})
	}

}
