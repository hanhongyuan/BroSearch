package bropen.search.config

import bropen.framework.core.DataDictData
import bropen.framework.core.security.DomainApplication
import bropen.search.Constants
import bropen.toolkit.utils.CollectionUtils
import bropen.toolkit.utils.grails.BeanUtils
import bropen.toolkit.utils.grails.DbUtils
import grails.core.DefaultGrailsApplication

/**
 * 文档索引配置
 */
class Searchable {

	/** 所属应用 */
	DomainApplication application

	/** 所属文档类 */
	String docClass

	/** 索引名 */
	String index

	/** 文档标题模板 */
	String title

	/** 文档摘要模板 */
	String summary

	/** 文档url模板 */
	String url

	/** 文档是否允许公共访问 */
	Boolean unclassified = Boolean.FALSE

	/** 文档分类：取自对应数据字典下的 id，eg: ';xxx;xxx;' */
	String categories

	// 搜索内容范围
	static final Integer QUERY_DOC_CONTENT = 1     // 仅在文档内容中搜索
	static final Integer QUERY_DOC_ATTACHMENT = 2  // 仅在文档附件中搜索
	// 搜索时间范围
	static final Integer QUERY_DATERANGE_TWOWEEKS = 5     // 2周内
	static final Integer QUERY_DATERANGE_THREEMONTHS = 10 // 3个月
	static final Integer QUERY_DATERANGE_THISYEAR = 15    // 今年
	static final Integer QUERY_DATERANGE_LASTYEARS = 20   // 去年以前
	static final Integer QUERY_DATERANGE_CUSTOM = 30      // 自定义

	/** 是否禁用 */
	Boolean disabled = Boolean.FALSE

	/** 是否自定义 */
	Boolean custom = Boolean.FALSE

	/** 备注 */
	String notes

	static hasMany = [fields: SearchableField]

	/** 时间戳... */
	Date dateCreated
	Date lastUpdated
	String updatedBy
	String createdBy

	///////////////////////////////////////////////////////////////////////////

	static constraints = {
		application(nullable: true, unique: "docClass")
		docClass(nullable: true)
		index(nullable: false, unique: true, blank: false)
		url(nullable: false, blank: false)
		categories(nullable: true, maxSize: 500)
		disabled(nullable: false)
		custom(nullable: false)
		unclassified(nullable: false)
		title(nullable: true, maxSize: Constants.FIELD_TITLE_MAXSIZE)
		summary(nullable: true, maxSize: Constants.FIELD_SUMMARY_MAXSIZE)
		notes(nullable: true, maxSize: 500)

		createdBy(editable: false, nullable: true, maxSize: 100)
		dateCreated(editable: false, format: "yyyy-MM-dd HH:mm")
		updatedBy(editable: false, nullable: true, maxSize: 100)
		lastUpdated(editable: false, format: "yyyy-MM-dd HH:mm")
	}

	static mapping = {
		version false
		table "brosea_searchable"
		fields lazy: false
		index column: "index_name"
	}

	def beforeInsert() {
		if (!this.application)
			this.application = DomainApplication.current()
	}

	///////////////////////////////////////////////////////////////////////////

	String toString() {
		return docClass
	}

	/**
	 * 获得所有文档分类
	 *
	 * @param options.clazz 加上该参数，默认会返回该类所属分类（含切面）信息
	 * @param options.main 是否主分类，默认取所有（包含主分类、非主分类），如果为 true 则参数 aspect 无效
	 * @param options.aspect 是否切面，默认取所有（包含切面、非切面）
	 * @param options.disabled 是否禁用，默认false
	 * @param options.parentId 分类父节点Id，如果有参数 clazz，则本参数无效
	 * @return [id: 分类ID（数据字典数据的ID）, name: 分类名称, code: 分类代码（数据字典数据的key）, parentId: 父分类ID（数据字典数据的父节点ID）]
	 */
	static List<Map> getAllCategories(Map options = [clazz: null, main: null, aspect: null, disabled: false, parentId: null]) {
		List<Long> parentIds = []
		if (options?.clazz) {
			String categories = Searchable.executeQuery(
					"select categories from bropen.search.config.Searchable where docClass=?",
					[((Class) options.clazz).name])[0]
			if (!categories) return null
			parentIds = categories.tokenize(";")*.toLong()
		}

		String hql = """select new map(id as id, key as code, val as name, parent.id as parentId)
				from bropen.framework.core.DataDictData where dataDict.code=? and disabled=?"""
		List queryParams = [Constants.DD_CATEGORIES, !!options?.disabled]
		if (options?.main instanceof Boolean) {
			hql += " and extVal2=?"
			queryParams << (options.main ? "1" : "0")
		} else if (options?.aspect instanceof Boolean) {
			hql += " and extVal1=?"
			queryParams << (options.aspect ? "1" : "0")
		}
		List<Map> list = (List<Map>) DataDictData.executeQuery(hql, queryParams)

		if (parentIds || options?.parentId) {
			Long parentId
			Map<Long, List<Map>> map = CollectionUtils.groupBy(list, "parentId")
			if (parentIds) {
				list = CollectionUtils.findAll(list, 'id', parentIds)
			} else {
				parentId = options.parentId
				list = map.containsKey(parentId) ? map[parentId] : []
			}
			// 所有下级
			if (list) {
				for (int i = 0; i < list.size(); i++) {
					parentId = (Long) list[i].id
					if (parentId && map.containsKey(parentId))
						list.addAll(map[parentId])
				}
			}
		}
		return list
	}

	/**
	 * 获取分类名称
	 */
	List<String> getCategoryNames() {
		List result = null
		if (this.categories) {
			result = DataDictData.executeQuery(
					DbUtils.assembleInLisHql("id", this.categories.tokenize(";"),
							[prefix: "select val from bropen.framework.core.DataDictData where "]))
		}
		return result
	}

	/**
	 * 获取分类下索引名称
	 *
	 * @param categoryId 分类id
	 */
	static List<String> getCategoryIndexes(Long categoryId) {
		(List<String>) Searchable.executeQuery(
				"select index from bropen.search.config.Searchable where disabled = ? and categories like ?",
				[false, "%;${categoryId};%"])
	}

	/**
	 * 获得当前应用中所有可配置索引的 Domain 类列表
	 */
	static List<String> getDomainClasses() {
		DefaultGrailsApplication grailsApplication = BeanUtils.getBean("grailsApplication")
		List<String> clazzNames = grailsApplication.domainClasses*.clazz*.name.sort()
		for (int i = clazzNames.size() - 1; i >= 0; i--) {
			if (clazzNames[i].startsWith("grails.")) {
				clazzNames.remove(i)
			} else if (clazzNames[i].startsWith("bropen.")) {
				clazzNames.add(clazzNames.remove(i))
			}
		}
		return clazzNames
	}

	/**
	 * @see SearchableService#getSearchableClazzes
	 */
	static List<Map> getSearchableClazzes(Map options = [all: false, disabled: false]) {
		SearchableService searchableService = BeanUtils.getBean("searchableService")
		searchableService.getSearchableClazzes(options)
	}

	/**
	 * @see SearchableService#getDomainProperties
	 */
	Map<String, Map<String, List<Map>>> getDomainProperties() {
		Class clazz = Class.forName(this.docClass)
		SearchableService searchableService = BeanUtils.getBean("searchableService")
		searchableService.getDomainProperties(clazz, (this.id ? this : null))
	}

}
