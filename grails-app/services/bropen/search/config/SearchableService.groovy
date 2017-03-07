/* Copyright © 2016 北京博瑞开源软件有限公司版权所有。*/
package bropen.search.config

import bropen.framework.core.DataDictData
import bropen.framework.core.osm.Employee
import bropen.framework.core.osm.Organization
import bropen.framework.core.security.DomainApplication
import bropen.search.annotation.Searchable as SearchableAnnotation
import bropen.search.annotation.SearchableField as SearchableFieldAnnotation
import bropen.search.es.EsApiWrapper
import bropen.search.es.EsCoreService
import bropen.search.es.EsIndexService
import bropen.search.orm.HibernateEventListener
import bropen.toolkit.utils.CollectionUtils
import bropen.toolkit.utils.StringUtils
import bropen.toolkit.utils.grails.BeanUtils
import bropen.toolkit.utils.grails.DbUtils
import grails.util.GrailsNameUtils
import org.grails.core.artefact.DomainClassArtefactHandler
import grails.core.DefaultGrailsApplication
import grails.core.GrailsClass
import grails.converters.JSON

@grails.compiler.GrailsCompileStatic
@grails.transaction.Transactional(readOnly = true)
class SearchableService {

	/**
	 * 初始化
	 */
	@grails.transaction.Transactional
	void bootStrapInit() {
		if (!EsCoreService.enabled()) return
		DomainApplication app = DomainApplication.current()
		// 更新老的
		List<Searchable> searchables = Searchable.findAllByApplication(app)
		for (Searchable searchable in searchables) {
			if (!searchable.custom)
				updateSearchable(searchable)
		}
		// 插入新的
		DefaultGrailsApplication grailsApplication = BeanUtils.getBean("grailsApplication") as DefaultGrailsApplication
		for (GrailsClass grailsClass in grailsApplication.getArtefacts(DomainClassArtefactHandler.TYPE)) {
			if (isSearchable(grailsClass.clazz)
					&& !CollectionUtils.find(searchables, "docClass", grailsClass.clazz.name)) {
				createSearchable(app, grailsClass.clazz, null)
			}
		}
		// 更新实时监听的类
		HibernateEventListener.reloadSearchableClasses()
		// 更新可用于搜索的索引列表
		//EsSearchService.setDocumentIndexes(getSearchableClazzes([all: true, disabled: false])*.get("index").toSet() as Set<String>)
	}

	/**
	 * 添加文档类到索引中（一般是没有 Searchable 注解的类）
	 * @param classNames
	 */
	@grails.transaction.Transactional
	void addSerchableClazzes(List<String> classNames) {
		DomainApplication app = DomainApplication.current()
		for (String className in classNames) {
			addSearchable(app, Class.forName(className))
		}
	}

	/**
	 * 添加一个文档类到索引中（一般是没有 Searchable 注解的类）
	 */
	@grails.transaction.Transactional
	void addSearchable(DomainApplication app, Class clazz, Map customProps = null) {
		if (!getSearchableClazzes([all: false, disabled: null])*.docClass.contains(clazz.name)) {
			//Searchable searchable =
			createSearchable(app, clazz, customProps)
			HibernateEventListener.addSearchableClass(clazz)
			//EsSearchService.addDocumentIndex(searchable.index)
		}
	}

	/**
	 * 重置索引配置
	 */
	@grails.transaction.Transactional
	void resetSearchable(Searchable searchable) {
		searchable.custom = false
		updateSearchable(searchable)
	}

	private Searchable createSearchable(DomainApplication app, Class clazz, Map customProps) {
		Map<String, Map> allpropsAnn = getDomainProperties(clazz)
		Searchable searchable = setSearchableProps(new Searchable(), allpropsAnn.get(clazz.name), customProps)
		if (!searchable.save()) println searchable.errors
		updateSearchableFields(searchable, clazz, null, allpropsAnn)
		return searchable
	}

	private Searchable setSearchableProps(Searchable searchable, Map propsAnn, Map customProps) {
		searchable.properties = propsAnn
		if (customProps) {
			searchable.properties = customProps
			searchable.custom = true
		}
		// url
		if (customProps?.containsKey("url"))
			searchable.url = generateUrl(searchable.class, (String) customProps.remove("url"))
		if (!searchable.url) {
			throw new RuntimeException("Document class [${searchable.class.name}] dosn't have a url!")
		}
		// categories
		Long[] categoryIds = (Long[]) (getCategoryIds(customProps?.get("categories")) ?: propsAnn.categories)
		searchable.categories = categoryIds ? ";${categoryIds.join(";")};" : null
		// title && summary
		String[] fields = ["title", "summary"]
		for (String field in fields) {
			String[] val = (String[]) (customProps?.get(field) ?: propsAnn[field])
			searchable[field] = val ? StringUtils.toJson(val) : null
		}
		return searchable
	}

	/**
	 * 获得数据字典的数据ID列表
	 * @param categories 由数据字典的数据ID或key组成的数组或集合
	 */
	private Long[] getCategoryIds(Object categories) {
		if (!categories) return null
		List<Long> result = []
		List<String> ddKeys = []
		if (categories instanceof Collection)
			categories = ((Collection) categories).toArray()
		for (Object category in (Object[]) categories) {
			if (category.toString().isInteger()) {
				result << category.toString().toLong() // 数据字典的数据ID
			} else {
				ddKeys << (category as String) // 数据字典的数据代码（key）
			}
		}
		if (ddKeys) {
			result.addAll((List<Long>) DataDictData.executeQuery(DbUtils.assembleInLisHql(
					"key", ddKeys, [prefix: "select id from bropen.framework.core.DataDictData where "])))
		}
		return (Long[]) result.toArray()
	}

	private void updateSearchable(Searchable searchable) {
		String docClass = searchable.docClass
		Class clazz = Class.forName(docClass)
		Map<String, Map> allpropsCfg = getDomainProperties(clazz, searchable),
						 allpropsAnn = getDomainProperties(clazz)
		setSearchableProps(searchable, allpropsAnn.get(docClass), null)
		if (!searchable.save()) println searchable.errors
		updateSearchableFields(searchable, clazz, allpropsCfg, allpropsAnn)
	}

	private void updateSearchableFields(Searchable searchable, Class clazz,
										Map<String, Map> allpropsCfg, Map<String, Map> allpropsAnn) {
		Integer fildsSize = searchable.fields?.size() ?: 0
		List<SearchableField> fields = generateFields(searchable, clazz, allpropsCfg, allpropsAnn)
		// 增、改
		for (SearchableField sf in fields) {
			if (!sf.id) searchable.addToFields(sf)
			if (!sf.save()) println sf.errors
		}
		// 删
		if (fildsSize > 0 && fields.size() != fildsSize) {
			for (SearchableField sf in (searchable.fields - fields)) {
				searchable.removeFromFields(sf)
				sf.delete()
			}
		}
	}

	/**
	 * 根据已有的索引配置和注解，生成属性索引列表
	 */
	List<SearchableField> generateFields(Searchable searchable, Class clazz,
										 Map<String, Map> allpropsCfg, Map<String, Map> allpropsAnn,
										 String fieldPath = null, List<Class> updatedClazzes = null,
										 List<SearchableField> result = null) {
		assert searchable || clazz
		if (result == null) result = (List<SearchableField>) []
		fieldPath = fieldPath ?: ""
		Map propsCfg = allpropsCfg?.get(fieldPath ?: clazz.name)
		Map propsAnn = allpropsAnn?.get(fieldPath ?: clazz.name)
		generateRefFields(searchable, clazz, propsCfg, propsAnn, allpropsCfg, allpropsAnn, fieldPath, "excludes", "disabled", result)
		generateRefFields(searchable, clazz, propsCfg, propsAnn, allpropsCfg, allpropsAnn, fieldPath, "mappingSqls", "mappingSql", result)
		generateRefFields(searchable, clazz, propsCfg, propsAnn, allpropsCfg, allpropsAnn, fieldPath, "mappings", "mappingProps", result)
		generateRefFields(searchable, clazz, propsCfg, propsAnn, allpropsCfg, allpropsAnn, fieldPath, "children", "child", result, updatedClazzes)
		return result
	}

	private void generateRefFields(Searchable searchable, Class clazz,
								   Map<String, List<Map>> propsCfg, Map<String, List<Map>> propsAnn,
								   Map<String, Map> allpropsCfg, Map<String, Map> allpropsAnn,
								   String fieldPath, String propskey, String fieldkey,
								   List<SearchableField> result, List<Class> updatedClazzes = null) {
		// 记录“父-子-孙-...”这一条线上的 domain 类
		List<Class> updatedClazzes2 = []
		if (updatedClazzes) updatedClazzes2.addAll(updatedClazzes)
		updatedClazzes2.add(clazz)

		// 插入、更新
		for (Map pa in propsAnn.get(propskey)) {
			Class childClazz = pa.reftype as Class
			if (!updatedClazzes2.contains(childClazz)) {
				String fieldName = fieldPath + (fieldPath != "" ? "." : "") + pa.name
				SearchableField field = propsCfg ? CollectionUtils.find(searchable.fields, "name", fieldName) : null
				if (null == field) {
					field = new SearchableField(searchable: searchable, docClass: clazz.name, name: fieldName)
				}
				if (pa.get(fieldkey) != null && fieldkey == "mappingProps" && !(pa.get(fieldkey) instanceof String)) {
					field[fieldkey] = StringUtils.toJson(pa[fieldkey])
				} else {
					field[fieldkey] = pa[fieldkey]
				}
				result << field
				// 递归子文档
				if (propskey == "children" && pa.child == 1) {
					generateFields(searchable, childClazz, allpropsCfg, allpropsAnn, fieldName, updatedClazzes2, result)
				}
			}
		}
	}

	/**
	 * 解析 Domain 类的属性，并分类返回
	 *
	 * @param searchableCfg 如果为空，则根据注解计算
	 *
	 * @return [className: [parent: 父文档属性, ordinaries: 普通属性, children: 子文档属性,
	 * 				mappingSql: 含有mappingSql属性, mappings: 含有mapping的属性], ...]
	 */
	Map getDomainProperties(Class clazz, Searchable searchableCfg = null,
							Map<String, Map> result = null, List<String> excludes = null,
							String fieldPath = null, List<String> classPath = null) {
		if (classPath == null) classPath = (List<String>) []
		classPath.add(clazz.name) // 记录引用栈上解析过的类，避免相同的类重复解析与死循环

		Map searchable = searchableCfg ? getSearchable(searchableCfg) : getSearchable(clazz)
		if (fieldPath == null) fieldPath = ""
		if (excludes == null) excludes = []
		if (searchable?.excludes) excludes.addAll(searchable?.excludes as List<String>)

		Map props = [clazz       : clazz,
					 docClass    : clazz.name,
					 index       : ((searchable?.index) ?: EsIndexService.getIndexName(clazz.name)),
					 disabled    : !!searchable?.disabled,
					 url         : searchable?.url,
					 parent      : null,
					 categories  : searchable?.categories,
					 title       : searchable?.title,
					 summary     : searchable?.summary,
					 unclassified: !!searchable?.unclassified,
					 ordinaries  : [], children: [], mappingSqls: [], mappings: [], excludes: []]
		if (null == result && null == searchableCfg) {
			props.url = generateUrl(clazz, (String) props.url)
		}

		if (result == null) result = [:]
		result.put(fieldPath ?: clazz.name, props)

		Map<String, Map> properties = BeanUtils.getPersistentProperties(clazz)
		// 父文档
		// 如：belongsTo = [foobar: Foobar] 或 belonsTo = Foobar 或 belongsTo = [Foo, Bar]
		// 注：父文档一般是不需要解析内容的，除非有 mappingXxx 或者配置为 child
		Map<String, Class> belongsTo = BeanUtils.getBelongsTo(clazz)
		String clazzPackage = clazz.name.substring(0, clazz.name.lastIndexOf("."))
		for (Map.Entry<String, Map> prop in properties.entrySet()) {
			String name = prop.key
			Map sfield
			if (searchableCfg) {
				sfield = getSearchableField(searchableCfg, fieldPath + (fieldPath ? "." : "") + prop.key)
			}
			if (sfield == null) { // 新加属性，searchableCfg 中未配置，重新根据 注解获取
				sfield = getSearchableField(clazz, prop.key)
			}
			Map p = (sfield ? [mappingProps: sfield.mappingProps, mappingSql: sfield.mappingSql, child: sfield.child] : [:])
			p.disabled = (sfield?.disabled || excludes.contains(prop.key)) ?: false
			p.putAll(prop.value)
			if (p.disabled) {
				(props.excludes as List) << p
			} else if (p.mappingSql) {
				(props.mappingSqls as List) << p
			} else if (p.mappingProps) {
				(props.mappings as List) << p
			} else if (p.isDomain) {
				Class reftype = p.reftype as Class
				if (classPath.contains(reftype.name)) continue
				// 父文档，如果未配置为子节点则不解析
				if (belongsTo.containsKey(name) && p.child != 1) {
					if (!props.parent)
						props.parent = ([] as List<Map>)
					(props.parent as List) << p
					continue
				}
				Boolean samePackage = (reftype.name.substring(0, reftype.name.lastIndexOf(".")) == clazzPackage)
				if (samePackage && (p.child == null || p.child == -1)) p.child = 1
				if (p.child == 0 || p.child == 1) {
					// 子文档：如果是1，则索引所有数据，但是必须有反向关联到父文档的属性，否则应使用注解属性 mappingSql
					(props.children as List) << p
				} else {
					// 关联文档domain（如不同包下的）：仅索引ID 或 mappingXxx 数据
					if (!p.mappingProps) {
						if (p.type == Employee.class) {
							p.mappingProps = ["${name}Id@id", "${name}Name@name"]
							(props.mappings as List) << p
						} else if (p.type == Organization.class) {
							p.mappingProps = ["${name}Id@id", "${name}FullName@fullName"]
							(props.mappings as List) << p
						}
					}
				}
				// 递归子文档或关联文档
				if (result != null && p.child == 1) {
					List<String> excludes2 = []
					String fieldPath2 = fieldPath + (fieldPath ? "." : "") + name
					for (String exclude in excludes) {
						if (exclude && exclude.startsWith(fieldPath2))
							excludes2 << exclude.replaceAll("^" + fieldPath2 + ".", "")
					}
					List<String> classPath2 = [] // 注：这里不能用 classPath.clone()，部署到 tomcat 后异常
					classPath2.addAll(classPath)
					getDomainProperties(reftype, searchableCfg, result, excludes2, fieldPath2, classPath2)
				}
			} else {
				(props.ordinaries as List) << p
			}
		}

		return result
	}

	/**
	 * 格式化/生成打开文档的 URL 模板
	 * @param url 需要格式化的模板
	 */
	static String generateUrl(Class clazz, String url = null) {
		if (url) {
			int slash = url.tokenize("/").size()
			if (slash == 1) {
				url += "/show/{id}"
			} else if (slash == 2) {
				url += "/{id}"
			}
			url = url.replaceAll("/+", "/")
		} else {
			Class controller = BeanUtils.findControllerByDomain(clazz.name)
			if (controller) {
				url = "/" + GrailsNameUtils.getPropertyName(controller).replace("Controller", "") + "/show/{id}"
			}
		}
		return url
	}

	private Map getSearchable(Searchable searchable) {
		if (searchable) {
			return [disabled    : searchable.disabled,
					excludes    : [],
					index       : searchable.index ?: EsIndexService.getIndexName(searchable.docClass),
					url         : searchable.url,
					unclassified: searchable.unclassified,
					categories  : searchable.categories?.tokenize(";") as Long[],
					title       : (searchable.title ? JSON.parse(searchable.title) : null),
					summary     : (searchable.summary ? JSON.parse(searchable.summary) : null)]
		}
		return null
	}

	/**
	 * 获取指定类的注解
	 * @param clazz
	 * @return
	 */
	private SearchableAnnotation getSearchableAnnotation(Class clazz) {
		return clazz.getAnnotation(SearchableAnnotation)
	}

	private Map getSearchable(Class clazz) {
		SearchableAnnotation searchable = getSearchableAnnotation(clazz)
		if (searchable) {
			return [disabled    : searchable.disabled(),
					excludes    : searchable.excludes().toList(),
					index       : searchable.index() ?: EsIndexService.getIndexName(clazz),
					url         : searchable.url(),
					unclassified: searchable.unclassified(),
					categories  : getCategoryIds(searchable.categories()),
					title       : searchable.title(),
					summary     : searchable.summary()]
		}
		return null
	}

	private Map getSearchableField(Searchable searchable, String fieldName) {
		SearchableField field = CollectionUtils.find(searchable?.fields, "name", fieldName) as SearchableField
		if (field) {
			return [disabled    : field.disabled,
					mappingProps: field.mappingProps ? (JSON.parse(field.mappingProps) as String[]) : null,
					mappingSql  : field.mappingSql,
					child       : field.child ? 1 : 0]
		}
		return null
	}

	/**
	 * 获取指定类的某个属性的注解
	 * @param clazz
	 * @param fieldName
	 * @return
	 */
	private SearchableFieldAnnotation getSearchableFieldAnnotation(Class clazz, String fieldName) {
		try {
			return clazz.getDeclaredField(fieldName).getAnnotation(SearchableFieldAnnotation)
		} catch (NoSuchFieldException e) {
			return null
		}
	}

	private Map getSearchableField(Class clazz, String fieldName) {
		SearchableFieldAnnotation field = getSearchableFieldAnnotation(clazz, fieldName)
		if (field) {
			return [disabled    : field.exclude(),
					mappingProps: field.mapping(),
					mappingSql  : field.mappingSql(),
					child       : field.child()]
		}
		return null
	}

	private boolean isSearchable(Class clazz) {
		return clazz.getAnnotation(SearchableAnnotation)
	}

	String getIndexName(Class clazz) {
		return getSearchable(clazz)?.index ?: EsIndexService.getIndexName(clazz)
	}

	String getIndexName(String clazzName) {
		Class clazz = BeanUtils.loadClass(clazzName)
		if (clazz) {
			return getIndexName(Class.forName(clazzName))
		} else {
			// 其他应用的
			return Searchable.executeQuery(
					"select s.index from bropen.search.config.Searchable s where s.docClass=?", [clazzName])[0]
		}
	}

	/**
	 * 获取所有需要索引的domain类
	 *
	 * @param options.all 是否取所有应用。默认为 false。
	 * @param options.disabled 是否只取禁用或启用的，null 表示所有，默认为 false。
	 *
	 * @return [[index: xx, docClass: xx, disabled: xx], ...]
	 */
	List<Map> getSearchableClazzes(Map options = [all: false, disabled: false]) {
		List queryParams = []
		StringBuilder hql = new StringBuilder()
		if (!options) options = [all: false, disabled: false]

		hql << "select new map(s.index as index, s.docClass as docClass, s.disabled as disabled) from bropen.search.config.Searchable s"
		if (!options?.all) {
			hql << " where s.application=? "
			queryParams << DomainApplication.current()
		}
		if (options?.disabled instanceof Boolean) {
			hql << (queryParams ? " and" : " where") << " s.disabled=?"
			queryParams << options.disabled
		}
		return Searchable.executeQuery(hql.toString(), queryParams)
	}

}