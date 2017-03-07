/* Copyright © 2016-2017 北京博瑞开源软件有限公司版权所有。*/
package bropen.search.es

import bropen.search.Page
import bropen.search.config.SearchableService
import bropen.toolkit.utils.grails.BeanUtils
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.*
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.SortOrder

import static bropen.search.Constants.FIELD_ATTACHMENTS
import static bropen.search.Constants.FIELD_CATEGORIES
import static bropen.search.Constants.FIELD_PERMISSIONS
import static bropen.search.Constants.TYPE_USER
import static bropen.search.Constants.TYPE_ATTACHMENT
import static bropen.search.Constants.INDEX_USER

/**
 * ES 搜索服务
 */
@groovy.transform.CompileStatic
class EsSearchService {

	EsCoreService esCoreService

	// 搜索范围的索引集
	//private static Set<String> documentIndexes = []
	private static String[] documentIndexesArray = null

	// 更新 documentIndexesArray 的计时器与失效时间
	private static final Timer documentIndexesRefreshTimer = new Timer()
	private static Integer documentIndexesRefreshInterval = 5000 * 60

	/**
	 * 设置默认搜索范围
	 * @param indexes 搜索范围需要包含的索引集
	 */
	/*public static void setDocumentIndexes(Set<String> indexes) {
		if (documentIndexes != null) {
			documentIndexes = indexes
			documentIndexesArray = documentIndexes.toArray()
		}
	}*/

	/**
	 * 添加一个文档索引到 search 方法的默认的搜索范围中。
	 */
	/*public static void addDocumentIndex(String index) {
		if (documentIndexes != null) {
			documentIndexes.add(index)
			documentIndexesArray = documentIndexes.toArray()
		}
	}*/

	/**
	 * 从 search 方法的默认的搜索范围中删除一个文档索引。
	 */
	/*public static void removeDocumentIndex(String index) {
		if (documentIndexes != null) {
			documentIndexes.remove(index)
			documentIndexesArray = documentIndexes.toArray()
		}
	}*/

	/**
	 * 获得搜索范围的索引数组（每隔一段时间自动失效、重建）
	 */
	private String[] getDocumentIndexedArray() {
		if (documentIndexesArray == null) {
			// 重新计算索引列表
			SearchableService searchableService = BeanUtils.getBean("searchableService") as SearchableService
			List<Map> cfgs = searchableService.getSearchableClazzes([all: true, disabled: false])
			//documentIndexes = cfgs*.index as Set<String>
			documentIndexesArray = cfgs*.index.toArray() as String[]
			// 设置计时器
			documentIndexesRefreshTimer.schedule(new TimerTask() {
				@Override
				public void run() {
					//EsSearchService.@documentIndexes = null
					//EsSearchService.@documentIndexesArray = null
					documentIndexesArray = null
				}
			}, documentIndexesRefreshInterval)
		}
		return documentIndexesArray
	}

	/**
	 * 自定义搜索<p>
	 *
	 * @param options.query 类型为 QueryBuilder 的查询条件（Query）
	 * @param options.postFilter 类型为 QueryBuilder 的过滤器（Post filter）
	 * @param options.max 每页的最大数据条数，如果不大于0，则为所有
	 * @param options.offset /page 当前页的数据开始序号，或当前页号
	 * @param options.index /indexes 索引名或索引名称列表。
	 * 		如果options中没有本参数则取默认的文档索引；如果为空则在所有es索引中搜索；否则仅在指定的索引中搜索。
	 * @param options.type /types 文档类型，String或String[]
	 * @param options.agg /aggs 聚合条件集，格式为 Map[别名：属性名] 或 Map 列表
	 * @param options.fetchSourceIncludes 返回的文档字段列表
	 * @param options.fetchSourceExcludes 不需要返回的文档字段列表
	 * @param options.sort 排序列表，类型为 List<Map<String, Object>>；
	 * 		其中 Map 的 key 为字段名（可以为"aa.bb"），值为 asc、desc 或 数字 1、-1 表示升、降序；
	 * 		如果排序列是字符串（长度小于256），则字段名应该加上后缀 .raw，如 name.raw。
	 * @return Page对象。注意返回数据中的时间是UTC格式的字符串，需先调用 DateUtils.parse(..) 转成Date类型后再使用。
	 */
	public Page search(Map<String, Object> options) {
		Integer max = (options?.max != null ? options.max : esCoreService.getQueryPageSize()) as Integer,
				page = (max <= 0) ? 1 : options?.page as Integer,
				offset = (max <= 0) ? 0 : (options?.offset ?: (page > 1 ? (page - 1) * max : 0)) as Integer
		if (options?.agg) {
			if (!options.aggs)
				options.aggs = []
			((List) options.aggs) << options.remove("agg")
		}
		if (page == null) page = (offset / max).toInteger() + 1
		Page result = new Page(page: page, offset: offset, max: max, total: 0L, hits: [] as SearchHit[])

		String[] indexes, types
		if (options?.index) {
			indexes = [(String) options.index]
		} else if (options?.indexes) {
			if (options.indexes instanceof String[])
				indexes = (String[]) options.indexes
			else if (options.indexes instanceof Collection)
				indexes = (options.indexes as Collection).toArray()
		} else if (!options?.index && !options?.indexes) {
			indexes = getDocumentIndexedArray()
		}
		if (options?.type) {
			types = [(String) options.type]
		} else if (options?.types) {
			if (options.types instanceof String[])
				types = (String[]) options.types
			else if (options.types instanceof Collection)
				types = (options.types as Collection).toArray()
		}

		Client client = esCoreService.getClient()
		SearchRequestBuilder builder = indexes ? client.prepareSearch(indexes) : client.prepareSearch()
		if (types) builder.setTypes(types)
		if (options?.query) builder.setQuery(options.query as QueryBuilder)
		//if (!options?.query) builder.setQuery(QueryBuilders.matchAllQuery())
		if (options?.postFilter) builder.setPostFilter(options.postFilter as QueryBuilder)
		if (max > 0) builder.setFrom(offset).setSize(max)
		if (options?.fetchSourceIncludes || options?.fetchSourceExcludes)
			builder.setFetchSource(options?.fetchSourceIncludes as String[], options?.fetchSourceExcludes as String[])
		if (options?.aggs) {
			for (Object agg in (List) options.aggs) {
				if (agg instanceof AggregationBuilder) {
					builder.addAggregation((AggregationBuilder) agg)
				} else if (agg instanceof Map) {
					for (Map.Entry<String, String> e in ((Map) agg).entrySet())
						builder.addAggregation(AggregationBuilders.terms(e.key).field(e.value).order(Terms.Order.term(true)))
				}
			}
		}
		if (options?.sort) {
			for (Map map in (List<Map>) options.sort) {
				// 长度小于 256 字符串都加了 xxx.raw 字段，用来排序，见 createIndexIfNotExists 的 defaultMapping
				for (Map.Entry<String, Object> e in map.entrySet())
					builder.addSort(e.key, (e.value == 1 || e.value == "asc") ? SortOrder.ASC : SortOrder.DESC)
			}
		}
		// LoggerFactory.getLogger('org.hibernate.SQL').level = Level.DEBUG
		if (log.isTraceEnabled()) log.trace("SearchRequestBuilder:\n" + builder)
		SearchResponse response = builder.get()
		SearchHits hits = response.getHits()
		if (log.isTraceEnabled()) log.trace("hits.total == " + hits.totalHits())
		result.aggs = response.getAggregations()?.asMap()
		result.total = hits.totalHits()
		result.hits = hits.getHits()
		return result
	}

	/**
	 * 根据 userId 搜索文档<p>
	 *
	 * @param userId
	 * @param options.queryBuilders 类型为 QueryBuilder 或字符串的查询条件集合
	 * @param options.postFilter 类型为 QueryBuilder 或字符串的过滤器（Post filter）
	 */
	public Page searchDocuments(Long userId, Map<String, Object> options = null) {
		QueryBuilder query = EsApiWrapper.termsLookupQuery(FIELD_PERMISSIONS, INDEX_USER, TYPE_USER, userId.toString(), "uuids")
		if (!options)
			options = [:]
		if (!options.queryBuilders)
			options.queryBuilders = []
		((List) options.queryBuilders).add(0, query)
		return searchDocuments(options)
	}

	/**
	 * 搜索文档<p>
	 *
	 * @param options.queryBuilders 类型为 QueryBuilder 或字符串的查询条件集合
	 * @param options.postFilter 类型为 QueryBuilder 或字符串的过滤器（Post filter）
	 */
	public Page searchDocuments(Map<String, Object> options = null) {
		QueryBuilder query = null
		// queryBuilders
		if (options?.queryBuilders) {
			query = QueryBuilders.boolQuery()
			for (qb in ((List) options.queryBuilders)) {
				if (!qb) continue
				if (qb instanceof QueryBuilder) {
					query.must(qb as QueryBuilder)
				} else if (qb instanceof String) {
					query.must(QueryBuilders.boolQuery()
							.should(QueryBuilders.queryStringQuery(qb as String))
							.should(EsApiWrapper.hasChildQuery(TYPE_ATTACHMENT, QueryBuilders.queryStringQuery(qb as String))))
				}
			}
		}
		// Post filter
		QueryBuilder postFilter = options?.postFilter instanceof String ?
				QueryBuilders.queryStringQuery(options.postFilter as String) : (options?.postFilter as QueryBuilder)

		return search([query              : query, postFilter: postFilter,
					   index              : options?.index, indexes: options?.indexes,
					   page               : options?.page, offset: options?.offset, max: options?.max, sort: options?.sort,
					   agg                : options?.agg, aggs: options?.aggs,
					   fetchSourceIncludes: options?.fetchSourceIncludes,
					   fetchSourceExcludes: [FIELD_PERMISSIONS, FIELD_ATTACHMENTS, FIELD_CATEGORIES] as String[]])
	}

	/**
	 * 搜索子节点
	 *
	 * @param index 索引名
	 * @param type 父文档类型
	 * @param parentId 父文档的ID
	 */
	public Page searchChildren(String index, String type, Object parentId = null, Map<String, Object> options = null) {
		QueryBuilder query = parentId ? QueryBuilders.idsQuery().addIds(parentId.toString()) : QueryBuilders.matchAllQuery()
		query = EsApiWrapper.hasParentQuery(type, query)
		return search((options ?: [:]) + [query: query, index: index])
	}

}