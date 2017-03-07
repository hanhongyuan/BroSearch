/* Copyright © 2016-2017 北京博瑞开源软件有限公司版权所有。*/
package bropen.search

import bropen.search.config.Searchable
import bropen.search.es.EsApiWrapper
import bropen.search.es.EsSearchService
import bropen.toolkit.utils.CollectionUtils
import bropen.toolkit.utils.DateUtils
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders

import static bropen.search.Constants.FIELD_APPLICATION
import static bropen.search.Constants.FIELD_DOCNUMBER
import static bropen.search.Constants.FIELD_DATECREATED
import static bropen.search.Constants.FIELD_LASTUPDATED
import static bropen.search.Constants.FIELD_CREATEDBY
import static bropen.search.Constants.FIELD_UPDATEDBY
import static bropen.search.Constants.FIELD_CLASS
import static bropen.search.Constants.FIELD_ID
import static bropen.search.Constants.FIELD_TITLE
import static bropen.search.Constants.FIELD_URL
import static bropen.search.Constants.FIELD_SUMMARY
import static bropen.search.Constants.FIELD_PERMISSIONS
import static bropen.search.Constants.FIELD_PERMISSIONS_DEFAULT
import static bropen.search.Constants.FIELD_CATEGORIES
import static bropen.search.Constants.INDEX_USER
import static bropen.search.Constants.TYPE_USER
import static bropen.search.Constants.TYPE_DOCUMENT

/**
 * 搜索服务
 */
@grails.transaction.Transactional(readOnly = true)
class SearchService {

	EsSearchService esSearchService

	// ISO standard format
	private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"

	public Map search(Long userId, Map params) {
		List<Object> queryBuilders = []
		// 有权限的根据user.uuids查询，公共访问的按 document._permission_uuids=“none” 查询
		QueryBuilder query1 = EsApiWrapper.termsLookupQuery(FIELD_PERMISSIONS, INDEX_USER, TYPE_USER, userId.toString(), "uuids")
		QueryBuilder query2 = QueryBuilders.termQuery(FIELD_PERMISSIONS, FIELD_PERMISSIONS_DEFAULT)
		QueryBuilder query = QueryBuilders.boolQuery().should(query1).should(query2)
		queryBuilders << query

		// 结果类型：按 内容、附件 搜索
		if (params.type && params.q) {
			if (params.type.toInteger() == Searchable.QUERY_DOC_CONTENT) {
				queryBuilders << QueryBuilders.queryStringQuery(params.q)
			} else if (params.type.toInteger() == Searchable.QUERY_DOC_ATTACHMENT) {
				queryBuilders << QueryBuilders.hasChildQuery(Constants.TYPE_ATTACHMENT,
						QueryBuilders.queryStringQuery(params.q))
			}
		} else if (params.q) {
			queryBuilders << params.q
		}

		// 时间范围
		if (params.dateRange) {
			Integer dateRange = params.dateRange.toInteger()
			Date startDate = null, endDate = null
			QueryBuilder dateBuilder = QueryBuilders.rangeQuery(FIELD_DATECREATED)
			if (dateRange == Searchable.QUERY_DATERANGE_TWOWEEKS) {
				Calendar c = Calendar.getInstance()
				endDate = c.getTime()
				c.add(Calendar.DATE, -14)
				startDate = c.getTime()
			} else if (dateRange == Searchable.QUERY_DATERANGE_THREEMONTHS) {
				Calendar c = Calendar.getInstance()
				endDate = c.getTime()
				c.add(Calendar.DATE, -92)
				startDate = c.getTime()
			} else if (dateRange == Searchable.QUERY_DATERANGE_THISYEAR) {
				endDate = new Date()
				startDate = DateUtils.toYear(endDate)
			} else if (dateRange == Searchable.QUERY_DATERANGE_LASTYEARS) {
				endDate = DateUtils.toYear(new Date())
			} else if (dateRange == Searchable.QUERY_DATERANGE_CUSTOM) {
				if (params.startDate)
					startDate = DateUtils.parse((String) params.startDate)
				if (params.endDate)
					endDate = DateUtils.parse((String) params.endDate)
			}
			if (startDate) dateBuilder.from(DateUtils.format(startDate, DATE_FORMAT))
			if (endDate) dateBuilder.to(DateUtils.format(endDate, DATE_FORMAT))
			dateBuilder.format(DATE_FORMAT) // 否则容易ES异常 ElasticsearchParseException: failed to parse date field
			queryBuilders << dateBuilder
		}

		// 索引分类
		String[] indexes = null
		Long categoryId = params.category ? params.category.toLong() : null
		if (categoryId) {
			indexes = Searchable.getCategoryIndexes(categoryId)
			if (!indexes) return null
		}

		// 分类聚合
		Map agg = [:]
		List<Map> categories = Searchable.getAllCategories([aspect: true, parentId: categoryId])
		Set<Long> aggIds = []
		for (Map m in categories) {
			if (params["cg.d${m.id}"]) {
				queryBuilders << QueryBuilders.termQuery("${FIELD_CATEGORIES}.${m.code}.raw", params["cg.d${m.id}"])
			} else {
				aggIds << (Long) m.parentId
				agg.put("d${m.id}", "${FIELD_CATEGORIES}.${m.code}.raw")
			}
		}

		// 统一按用户uuids中权限和 document._permission_uuids 匹配查询（10条数据测试，此方案稍快）
		//Page page = esSearchService.searchDocuments(userId, [postFilter:params.q, indexes: (indexes as String[]), type: TYPE_DOCUMENT])
		Page page = esSearchService.searchDocuments(
				[queryBuilders      : queryBuilders,
				 agg                : agg,
				 indexes            : indexes,
				 type               : TYPE_DOCUMENT,
				 fetchSourceIncludes: [FIELD_APPLICATION, FIELD_CLASS, FIELD_URL,
									   FIELD_ID, FIELD_TITLE, FIELD_SUMMARY, FIELD_DOCNUMBER,
									   FIELD_DATECREATED, FIELD_LASTUPDATED, FIELD_CREATEDBY, FIELD_UPDATEDBY]])
		Map<String, Map<Object, Long>> aggs = page?.aggCounts()

		// 处理多层聚合分类结果
		if (aggs && aggIds.size() > 1) {
			aggs = normalizeAggs(aggs, categoryId)
		}

		Map result = [list      : page?.hits*.getSource(),
					  total     : page?.total,
					  categories: aggs]
		//println result
		return result
	}

	/**
	 * 处理多层聚合分类结果
	 * <p>分类中父级、子级分类同时存在，只显示父级分类；同时过滤掉只有一个分类项的分类。</p>
	 */
	private Map<String, Map<Object, Long>> normalizeAggs(
			Map<String, Map<Object, Long>> aggs, Long parentId,
			Map<String, Map<Object, Long>> result = null, Map<Long, List<Map>> categoriesMap = null) {
		if (result == null) result = [:]
		if (categoriesMap == null) {
			List<Map> categories = Searchable.getAllCategories()
			categoriesMap = (Map<Long, List<Map>>) CollectionUtils.groupBy(categories, "parentId")
		}
		for (Map m in categoriesMap[parentId]) {
			String did = "d${m.id}"
			if (aggs.containsKey(did) && aggs[did].size() > 1) {
				result[did] = aggs[did]
			} else if (categoriesMap.containsKey(parentId)) {
				normalizeAggs(aggs, (Long) m.id, result, categoriesMap) // 递归
			}
		}
		return result
	}
}
