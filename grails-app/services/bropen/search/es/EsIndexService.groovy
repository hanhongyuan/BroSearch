/* Copyright © 2016 北京博瑞开源软件有限公司版权所有。*/
package bropen.search.es

import bropen.search.Page
import bropen.toolkit.utils.DateUtils

import com.carrotsearch.hppc.cursors.ObjectObjectCursor
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse
import org.elasticsearch.action.admin.indices.refresh.RefreshAction
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequestBuilder
import org.elasticsearch.action.get.GetRequestBuilder
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.update.UpdateRequestBuilder
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.MappingMetaData
import org.elasticsearch.common.collect.ImmutableOpenMap
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.script.Script

import static bropen.search.Constants.LASTUPDATED_ATTACHMENT
import static bropen.search.Constants.LASTUPDATED_DOCUMENT
import static bropen.search.Constants.TYPE_LASTUPDATED
import static bropen.search.Constants.TYPE_DOCUMENT
import static bropen.search.Constants.TYPE_USER
import static bropen.search.Constants.FIELD_ATTACHMENTS
import static bropen.search.Constants.FIELD_PERMISSIONS

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder

import grails.converters.JSON
import java.util.concurrent.ExecutionException

/**
 * ES 索引读写服务
 */
@groovy.transform.CompileStatic
class EsIndexService {

	EsCoreService esCoreService
	EsSearchService esSearchService

	/** 每次推送的数据量 10M */
	private static Integer maxBulkSize = 10 << 20

	/**
	 * 获得文档类的索引名称
	 * <p>
	 * 计算规则为类全路径转换，'.' 转换成 '_'，大写字母 转化成下划线+小写字母，
	 * 如 foo.oa.hr.AskForLeave 转成 foo_oa_hr__ask_for_leave。</p>
	 */
	static String getIndexName(Class clazz) {
		return getIndexName(clazz.name)
	}

	/**
	 * 获得文档类的索引名称
	 */
	static String getIndexName(String clazzName) {
		return clazzName.replace(".", "_").replaceAll("[A-Z]", '_$0').toLowerCase()
	}

	/**
	 * 判断索引是否存在
	 *
	 * @param index 索引名
	 * @param client
	 * @return
	 */
	public Boolean isIndexExists(String index, Client client = null) {
		if (!client) client = esCoreService.getClient()
		IndicesExistsResponse response = client.admin().indices().prepareExists(index).get()
		return response.isExists()
	}

	/**
	 * 创建索引
	 * <p>相当于执行curl -XPOST "http://localhost:9200/index"</p>
	 *
	 * @param index 索引名
	 * @param options.defaultMapping 默认 mapping，类型为 Map 或者 true；
	 * 		如果为 true 则使用一个自动为多数字符串字段添加 not_analyzed 的 raw 字段的 defaultMapping，以便于排序和分组。
	 * @return 如果索引已存在或创建成功则返回 true，否则返回 false。
	 */
	public Boolean createIndexIfNotExists(String index, Map options = null) {
		Boolean success = true
		Client client = esCoreService.getClient()
		index = index.toLowerCase()
		if (!isIndexExists(index, client)) {
			CreateIndexResponse response = client.admin().indices().prepareCreate(index).get()
			success = response.isAcknowledged()
			if (success && options?.defaultMapping) {
				try {
					if (options.defaultMapping instanceof Map) {
						success = putMapping(index, "_default_", options.defaultMapping as Map)
					} else {
						success = putMapping(index, "_default_", defaultMapping)
					}
				} catch (Exception e) {
					success = false
					log.error("create default mapping for [${index}] error.", e)
					deleteIndex(index)
				}
			}
			log.debug("Create es index [${index}] " + (success ? "succeeded" : "failed"))
		}
		return success
	}

	/**
	 * 创建索引时的默认 mapping
	 * <p>可以修改；在调用 createIndexIfNotExists 时可选择是否启用。</p>
	 * <p>参考 https://www.elastic.co/guide/en/elasticsearch/reference/current/default-mapping.html</p>
	 */
	public static Map defaultMapping = [
			_default_: [
					dynamic_templates: [[
												strings: [
														match_mapping_type: "string",
														mapping           : [
																type  : "string",
																fields: [
																		raw: [
																				type        : "string",
																				index       : "not_analyzed",
																				ignore_above: 256]
																]
														]
												]
										]]
			]
	]

	/**
	 * 删除整个索引
	 * <p>相当于执行 curl -XDELETE "http://localhost:9200/index"</p>
	 *
	 * @param index 索引名
	 * @return 如果索引已存在或删除成功则返回 true，否则返回 false
	 */
	public Boolean deleteIndex(String index) {
		Boolean success = true
		Client client = esCoreService.getClient()
		index = index.toLowerCase()
		if (isIndexExists(index, client)) {
			DeleteIndexResponse response = client.admin().indices().prepareDelete(index).get()
			success = response.isAcknowledged()
			log.debug("Delete es index [${index}] " + (success ? "succeeded" : "failed"))
		}
		return success
	}

	/**
	 * 打开索引<p>
	 * @param indexes 多个索引名
	 * @return
	 */
	public Boolean openIndex(String... indexes) {
		Client client = esCoreService.getClient()
		OpenIndexResponse response = client.admin().indices().prepareOpen(indexes).get()
		return response.isAcknowledged()
	}

	/**
	 * 关闭索引
	 * @param indexes 多个索引名
	 * @return
	 */
	public Boolean closeIndex(String... indexes) {
		Client client = esCoreService.getClient()
		CloseIndexResponse cIndexResponse = client.admin().indices().prepareClose(indexes).get()
		return cIndexResponse.isAcknowledged()
	}

	/**
	 * 刷新索引，以便能够马上查到更新的数据
	 * <p>默认 es 每秒钟会自动刷新、或者根据索引的 refresh_interval 配置执行，一般不建议在程序中频繁调用。</p>
	 *
	 * @param indexes
	 */
	public void refresh(String... indexes) {
		if (indexes) {
			RefreshAction.INSTANCE.newRequestBuilder(esCoreService.getClient()).setIndices(indexes).get()
		} else {
			RefreshAction.INSTANCE.newRequestBuilder(esCoreService.getClient()).get()
		}
	}

	/**
	 * 删除索引下某种文档类型的所有文档
	 *
	 * @param index 索引名称
	 * @param type 文档类型
	 */
	public void deleteDocuments(String index, String type) {
		Client client = esCoreService.getClient()
		if (isIndexExists(index, client)) {
			// 删除子文档
			for (String t in getChildTypes(index, type)) {
				deleteDocuments(index, t)
			}
			// 删除文档
			Long count = EsApiWrapper.deleteDocuments(index, type, client)
			// 如果是主文档，则删除最后更新时间
			if (type == TYPE_DOCUMENT || type == TYPE_USER) {
				deleteDocuments(index, TYPE_LASTUPDATED)
			}
			log.debug("Delete typed data: index = ${index}, type = ${type}, deleted: ${count}")
		}
	}

	/**
	 * 获取文档类型的子类型
	 * @param index 索引名称
	 * @param type 父文档类型
	 * @return
	 */
	public List<String> getChildTypes(String index, String type) {
		List<String> types = []
		ImmutableOpenMap<String, MappingMetaData> mappings = getMappings(index)
		for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
			Map m = (Map) cursor.value.getSourceAsMap().get("_parent")
			if (m != null && type == m.type) types << cursor.key
		}
		return types
	}

	/**
	 * 获取索引下map结构的所有数据类型映射（mapping）
	 *
	 * @param index 索引名
	 */
	public Map<String, Map<String, Object>> getMappingsAsMap(String index)
			throws InterruptedException, ExecutionException {
		Map<String, Map<String, Object>> result = [:]
		ImmutableOpenMap<String, MappingMetaData> mappings = getMappings(index);
		for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
			result[cursor.key] = cursor.value.getSourceAsMap()
		}
		return result
	}

	/**
	 * 获取索引下的所有数据类型映射（mapping）
	 *
	 * @param index 索引名
	 */
	public ImmutableOpenMap<String, MappingMetaData> getMappings(String index)
			throws InterruptedException, ExecutionException {
		index = index.toLowerCase()
		GetMappingsResponse response = esCoreService.getClient().admin().indices().prepareGetMappings(index).get()
		ImmutableOpenMap<String, MappingMetaData> mapping = response.mappings().get(index)
		return mapping
	}

	/**
	 * 获取文档类型的数据类型映射（mapping）
	 *
	 * @param index 索引名
	 * @param type 文档类型
	 */
	public MappingMetaData getMapping(String index, String type) throws InterruptedException, ExecutionException {
		ImmutableOpenMap map = getMappings(index)
		return map.get(type)
	}

	/**
	 * 判断索引是否包含有 mapping
	 * @param index 索引名
	 * @return
	 */
	public Boolean hasMappings(String index) throws InterruptedException, ExecutionException {
		ImmutableOpenMap mapping = getMappings(index)
		return mapping?.size() > 0
	}

	/**
	 * 判断索引是否包含某种文档类型的 mapping
	 * @param index 索引名
	 * @param type 文档类型
	 */
	private Boolean hasMapping(String index, String type) throws InterruptedException, ExecutionException {
		return getMapping(index, type) != null
	}

	/**
	 * 给索引的一个文档类型（type）创建数据类型映射（mapping）
	 *
	 * <pre>生成mapping结构：<code> {*   "${当前type}": {*     "_all": {*       "analyzer": "ik_max_word",
	 *       "search_analyzer": "ik_max_word",
	 *       "term_vector": "no",
	 *       "store": true
	 *},
	 *     "_parent" : {*       "type": "${上级type}"
	 *},
	 *     "properties": {*       "name": {*         "type": "string",
	 *         "index" "no"
	 *}*}*}*}</code></pre>
	 *
	 * <p>需要注意的是，子文档类型必须先创建，然后再创建父文档类型，否则会报错。</p>
	 *
	 * @param index 索引名
	 * @param type 文档类型名
	 * @param parentType 上级文档类型
	 * @param notIndexFields 不设置mapping的属性集
	 * @return 数据映射存在或者创建成功则返回 true，否则 false
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public Boolean putMappingIfNotExists(String index, String type,
										 String parentType = null, List<String> notIndexFields = null)
			throws InterruptedException, ExecutionException {
		index = index.toLowerCase()
		if (!hasMapping(index, type)) {
			Map<String, Map> mapping = [:]
			mapping.put(type, ["_all": ["analyzer"       : "ik_max_word",
										"search_analyzer": "ik_max_word",
										"term_vector"    : "no",
										"store"          : true]])
			if (parentType) {
				mapping[type].put("_parent", ["type": parentType])
				//mapping[type].put("_routing", ["required": false])
			}
			if (notIndexFields) {
				Map properties = [:]
				for (String field in notIndexFields) {
					properties.put(field, ["index": "no", "type": "string"])
				}
				mapping[type].put("properties", properties)
			}
			// TODO +not_analyzed、raw for sort & group、include_in_all false
			/*if (type == TYPE_DOCUMENT) {
				Map properties = (mapping[type].get("properties") as Map) ?: [:]
				properties.put("_attachments", ["type"      : "nested",
												"properties": ["name": ["id": "long", "index": "not_analyzed"]]])
				mapping[type].put("properties", properties)
			}*/
			return putMapping(index, type, mapping)
		}
		return true
	}

	private Boolean putMapping(String index, String type, Map map) {
		Client client = esCoreService.getClient()
		PutMappingResponse response = client.admin().indices().preparePutMapping(index).setType(type).setSource(map).get()
		return response.isAcknowledged()
	}

	private Boolean putJsonMapping(String index, String type, String json) {
		Client client = esCoreService.getClient()
		PutMappingResponse response = client.admin().indices().preparePutMapping(index).setType(type).setSource(json).get()
		return response.isAcknowledged()
	}

	/**
	 * 批量索引文档
	 *
	 * @param index 索引名。当为字符串时为索引名；当为闭包时，会调用 index(doc) 获取索引名。
	 * @param type 类型名。当为字符串时为类型名；当为闭包时，会调用 type(doc) 获取文档类型。
	 * @param documents 文档列表
	 * @param options.parent 父节点ID。当为闭包时，会调用 parent(doc) 获取 parentId，否则则为 doc 中 parentId 所在的属性名。
	 * @param options.idGetter 生成id的闭包
	 * @param options.overwrite 是否强行覆盖文档所有字段，一般用于 init 文档时提升性能
	 * @return 分批次提交到es的状态，形如 [status: [成功失败状态], errors: [一一对应的失败原因]]
	 */
	public Map indexDocuments(Object index, Object type, List<Map> documents, Map options = null) {
		Map result = [status: [], errors: []]
		if (!documents) return result
		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		// 分批次索引
		Integer length = 0
		Boolean isParentField = null
		for (Map doc in documents) {
			if (isParentField == null)
				isParentField = doc.containsKey(options?.parent)
			doc = formatField(doc) as Map
			String source = new JSON(doc).toString()
			if (length > 0 && length + source.length() > maxBulkSize) {
				length = 0
				BulkResponse response = request.get()
				result.status << response.hasFailures()
				result.errors << response.buildFailureMessage()
			}
			length += source.length()
			buildDocument2Request(request, doc, index, type, (options?.idGetter ?: doc.id),
					(isParentField ? doc[options?.parent] : options?.parent), options?.overwrite as Boolean)
			/*IndexRequestBuilder builder = buildDocument(doc, index, type, (idGetter ?: doc.id), (isParentField ? doc[parent] : parent))
			request.add(builder.setSource(source))*/
		}
		BulkResponse response = request.get()
		result.status << response.hasFailures()
		result.errors << response.buildFailureMessage()
		return result
	}

	private IndexRequestBuilder buildDocument(Map doc, Object index, Object type, Object id, Object parent) {
		String indexName = ((index instanceof String ? index : (index as Closure).call(doc)) as String).toLowerCase(),
			   typeName = (type instanceof String ? type : (type as Closure).call(doc)),
			   docId = (id instanceof Closure ? id.call(doc) : id).toString(),
			   parentId = (parent instanceof Closure ? parent.call(doc) : parent)?.toString()
		IndexRequestBuilder builder = esCoreService.getClient().prepareIndex(indexName, typeName, docId)
		if (parentId != null) builder.setParent(parentId)
		return builder
	}

	private void buildDocument2Request(BulkRequestBuilder request, Map doc,
									   Object index, Object type, Object id, Object parent, Boolean overwrite) {
		String indexName = ((index instanceof String ? index : (index as Closure).call(doc)) as String).toLowerCase(),
			   typeName = (type instanceof String ? type : (type as Closure).call(doc)),
			   docId = (id instanceof Closure ? id.call(doc) : id).toString(),
			   parentId = (parent instanceof Closure ? parent.call(doc) : parent)?.toString()
		// 先判断文档是否存在：如果存在，则更新、并保留一些字段不更新；否则插入
		if (!overwrite && isDocumentExists(indexName, typeName, docId)) {
			Script script = EsApiWrapper.getScript(partial_update_document_script,
					[source: doc, preserved_fields: partial_update_document_preserved_fields])
			request.add(esCoreService.getClient().prepareUpdate(indexName, typeName, docId).setScript(script))
		} else {
			IndexRequestBuilder builder = buildDocument(doc, indexName, typeName, docId, parentId)
			request.add(builder.setSource(doc))
		}
	}

	private static String partial_update_document_script = """
		for (String field in preserved_fields) {
			if (ctx._source.containsKey(field) && !source.containsKey(field)) {
				source.put(field, ctx._source.get(field))
			}
			ctx._source = source
		}
	"""

	private static List<String> partial_update_document_preserved_fields = [FIELD_ATTACHMENTS, FIELD_PERMISSIONS]

	/**
	 * 保存文档或附件的最近更新时间
	 *
	 * @param index 索引名
	 * @param type 文档或附件，取值为 Constants.LASTUPDATED_ATTACHMENT or LASTUPDATED_DOCUMENT
	 * @param lastUpdated 最近更新时间
	 */
	public void saveLastUpdated(String index, String type, Date lastUpdated) {
		if (type == LASTUPDATED_ATTACHMENT || type == LASTUPDATED_DOCUMENT) {
			if (!hasMapping(index, TYPE_LASTUPDATED)) {
				Map<String, Map> mapping = [:]
				mapping.put(TYPE_LASTUPDATED,
						["properties": ["_text_": ["index": "no", "type": "string"], "_value_": ["index": "no", "type": "long"]]/*,
						 "_all"      : ["enabled": false]*/]) // 如果加上 _all，es5.1.1版本报 java.lang.IllegalArgumentException
				putMapping(index, TYPE_LASTUPDATED, mapping)
			}
			putMappingIfNotExists(index, TYPE_LASTUPDATED, null, ["_value_", "_text_"])
			indexDocument(index, TYPE_LASTUPDATED, type,
					[_value_: lastUpdated.getTime(), _text_: DateUtils.format(lastUpdated, "yyyy-MM-dd HH:mm:ss.SSS")])
		}
	}

	/**
	 * 获取文档或附件的最近更新时间
	 */
	public Date getLastUpdated(String index, String type) {
		if (type == LASTUPDATED_ATTACHMENT || type == LASTUPDATED_DOCUMENT) {
			if (isIndexExists(index)) {
				Long lastUpdated = getDocument(index, TYPE_LASTUPDATED, type)?._value_ as Long
				return lastUpdated ? new Date(lastUpdated) : DateUtils.parse("2011-01-01")
			} else {
				return DateUtils.parse("2011-01-01")
			}
		}
		return null
	}

	/**
	 * 批量索引文档
	 *
	 * @param documents 包含 index、type、id、parent、value 的文档列表，其中 value 为要索引的内容
	 */
	public Map indexDocuments(List<Map> documents) {
		Map result = [errors: [], messages: []]
		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		BulkResponse response
		Integer length = 0
		for (Map doc in documents) {
			doc.value = formatField(doc.value)
			String source = new JSON(doc.value).toString()
			if (length > 0 && length + source.length() > maxBulkSize) {
				length = 0
				response = request.get()
				result.errors << response.hasFailures()
				result.messages << (response.hasFailures() ? response.buildFailureMessage() : null)
			}
			length += source.length()
			buildDocument2Request(request, doc.value as Map, doc.index, doc.type, doc.id, doc.parent, false)
			/*IndexRequestBuilder builder = buildDocument(doc.value, doc.index, doc.type, doc.id, doc.parent)
			request.add(builder.setSource(source))*/
		}
		response = request.get()
		result.errors << response.hasFailures()
		result.messages << (response.hasFailures() ? response.buildFailureMessage() : null)
		return result
	}

	/**
	 * 插入或修改单个文档
	 */
	public Boolean indexDocument(Object index, Object type, Object id, Map doc, Object parent = null) {
		doc = formatField(doc) as Map
		BulkRequestBuilder request = esCoreService.getClient().prepareBulk()
		buildDocument2Request(request, doc, index, type, id, parent, false)
		return !request.get().hasFailures()
		/*IndexRequestBuilder builder = buildDocument(doc, index, type, id, parent)
		IndexResponse response = builder.setSource(doc).get()
		return response.isCreated()*/
	}

	/**
	 * 插入或修改单个文档
	 * @param doc 包含 index、type、id、parent、value 的文档，其中 Map value 为要索引的内容
	 */
	public Boolean indexDocument(Map doc) {
		doc.value = formatField(doc.value) as Map
		BulkRequestBuilder request = esCoreService.getClient().prepareBulk()
		buildDocument2Request(request, doc.value as Map, doc.index, doc.type, doc.id, doc.parent, false)
		return !request.get().hasFailures()
		/*IndexRequestBuilder builder = buildDocument(doc.value, doc.index, doc.type, doc.id, doc.parent)
		IndexResponse response = builder.setSource(doc.value).get()
		return response.isCreated()*/
	}

	/**
	 * 根据文档ID删除索引库中的文档
	 * <p>相当于执行命令：curl —XDELETE "http://localhost:9200/index/typename/1"</p>
	 *
	 * @param index 索引名称
	 * @param type 索引type名
	 * @param id 文档id
	 * @param parentId 父文档的id
	 */
	public Boolean deleteDocument(String index, String type, Object id, Object parentId = null) {
		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		deleteDocument_(client, request, index, type, id.toString(), parentId?.toString())
		BulkResponse response = request.get()
		log.debug("Delete document [" + index + "/" + type + "/" + id + "] "
				+ (response.hasFailures() ? "failed: " : "succeeded.")
				+ (response.hasFailures() ? response.buildFailureMessage() : ""))
		return !response.hasFailures()
	}

	/**
	 * 批量删除文档
	 *
	 * @param documents 包含 index、type、id、parent 的文档列表
	 */
	public Boolean deleteDocuments(List<Map> docs) {
		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		for (Map doc in docs) {
			deleteDocument_(client, request,
					(String) doc.index, (String) doc.type, doc.id.toString(), doc.parent?.toString())
		}
		BulkResponse response = request.get()
		log.debug("Delete [" + docs.size() + "] documents "
				+ (response.hasFailures() ? "failed: " : "succeeded.")
				+ (response.hasFailures() ? response.buildFailureMessage() : ""))
		return !response.hasFailures()
	}

	/**
	 * 批量删除文档
	 */
	public Boolean deleteDocuments(String index, String type, List<Object> ids) {
		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		for (Object id in ids) {
			deleteDocument_(client, request, index, type, id.toString(), null)
		}
		BulkResponse response = request.get()
		log.debug("Delete [" + ids.size() + "] documents [" + index + "/" + type + "] "
				+ (response.hasFailures() ? "failed: " : "succeeded.")
				+ (response.hasFailures() ? response.buildFailureMessage() : ""))
		return !response.hasFailures()
	}

	private void deleteDocument_(Client client, BulkRequestBuilder request,
								 String index, String type, String id, String parentId) {
		// 删除子文档
		List<String> types = getChildTypes(index, type)
		if (types) {
			Page page = esSearchService.searchChildren(index, type, id, [max: 0] as Map<String, Object>)
			if (page.total > 0) {
				for (SearchHit hit in page.hits) {
					log.debug("Delete child document [${index}/${hit.getType()}/${hit.getId()}] of [${type}/${id}].")
					deleteDocument_(client, request, index, hit.getType(), hit.getId(), id)
				}
			}
		}
		// 删除文档
		DeleteRequestBuilder builder = client.prepareDelete(index, type, id) //.setRefresh(true)
		if (parentId) builder.setParent(parentId)
		request.add(builder)
	}

	/**
	 * 删除文档属性<p>
	 *
	 * @param id 文档ID
	 * @param field 属性名称
	 */
	public Boolean deleteDocumentField(String index, String type, Object id, String field) {
		deleteDocumentField(index, type, id, [field])
	}

	/**
	 * 删除文档属性
	 * <p>注：脚本格式 {@code 'ctx._source.processInstance.remove("id")' }</p>
	 *
	 * @param field 属性名称路径，如 ["processInstance", "id"]
	 */
	public Boolean deleteDocumentField(String index, String type, Object id, List<String> field) {
		StringBuilder sb = new StringBuilder("ctx._source")
		for (int i = 0; i < field.size() - 1; i++) {
			sb << "." << field[i]
		}
		sb << '.remove("' << field[field.size() - 1] << '")'
		UpdateResponse response = esCoreService.getClient().prepareUpdate(index, type, id.toString())
				.setScript(EsApiWrapper.getScript(sb.toString(), null)).get()
		log.debug("Delete document [" + index + "/" + type + "/" + id + "]'s field [" + field + "].")
		return response.shardInfo.failed == 0
	}

	/**
	 * 删除集合属性的部分元素
	 * <p>如删除 ID 为 3246608 的文档下、ID 为 3368127 或 3368139 的流程任务：<br/>
	 * {@code ctx.esIndexService.deleteDocumentFieldItems ('boe_bpm_ct_ctvendormasterdata', 'document', '3246608', ["processInstance", "tasks"], [3368127, 3368139])}</p>
	 *
	 * @param id 文档ID
	 * @param field 复杂属性名称路径，如 ["processInstance", "tasks"]
	 * @param itemIds 元素的 id 列表，注意如果元素 id 是 Long 型的，这里不能是字符串类型
	 */
	public Map deleteDocumentFieldItems(String index, String type, Object id, List<String> field, List<String> itemIds) {
		Map<String, Object> params = ([ids: itemIds] as Map<String, Object>)
		String condition = "it.id in ids"
		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk() // 为了返回是否成功，用 bulk 方式执行
		String script = assembleDeleteScript(field, condition)
		request.add(client.prepareUpdate(index, type, id.toString())
				.setScript(EsApiWrapper.getScript(script, params)))
		BulkResponse response = request.get()
		return [error: response.hasFailures(), message: response.buildFailureMessage()]
	}

	/**
	 * 删除集合属性的部分元素
	 * <p>如删除多个文档里的一些流程任务：{@code
	 * [[index:'boe_bpm_ct_ctvendormasterdata', type:'document', id:3246608, field:["processInstance", "tasks"], itemIds:[3368790, 3372543]],
	 *    [index:'boe_bpm_ct_ctvendormasterdata', type:'document', id:3011343, field:["processInstance", "tasks"], itemIds:[3088117, 3088120]],
	 *    [index:'boe_bpm_ct_ctvendormasterdata', type:'document', id:30113423, field:["processInstance", "tasks"], itemIds:[3088117, 3088120]]
	 *   ]}</p>
	 *
	 * @param list 包含 index、type、id、field、itemIds 的 Map 列表
	 */
	public Map deleteDocumentFieldItems(List<Map> list) {
		if (!list) return null
		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		String condition = "it.id in ids"
		for (Map m in list) {
			Map params = [ids: m.itemIds]
			String script = assembleDeleteScript(m.field as List, condition)
			request.add(client.prepareUpdate(m.index as String, m.type as String, m.id.toString())
					.setScript(EsApiWrapper.getScript(script, params)))
		}
		BulkResponse response = request.get()
		return [error: response.hasFailures(), message: response.buildFailureMessage()]
	}

	/**
	 * 给集合属性添加元素
	 * <p>示例：{@code
	 z     * ctx.esIndexService.addDocumentFieldItem ('boe_bpm_ct_ctvendormasterdata', 'document', '3246608', ["processInstance", "tasks" ], ["class": "aaaa", id: 3373384])
	 *}</p>
	 */
	public Map addDocumentFieldItem(String index, String type, Object id, List<String> field, Map item) {
		Map<String, Object> params = ["obj": formatField(item)]
		String scriptDelete = assembleDeleteScript(field, "it.id == obj.id"),
			   scriptAdd = assembleAddItemScript(field, "obj", false)

		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		request.add(client.prepareUpdate(index, type, id.toString())
				.setScript(EsApiWrapper.getScript(scriptDelete, params)))
		request.add(client.prepareUpdate(index, type, id.toString())
				.setScript(EsApiWrapper.getScript(scriptAdd, params)))
		BulkResponse response = request.get()
		return [error: response.hasFailures(), message: response.buildFailureMessage()]
	}

	/**
	 * 给集合属性添加多个元素
	 * <p>示例：{@code
	 * ctx.esIndexService.addDocumentFieldItems (
	 * ' boe_bpm_ct_ctvendormasterdata ' , ' document ' , ' 3246608 ' ,
	 * [ " processInstance " , " tasks " ] ,
	 * [[ " class " : " bbb " , id : 3373384 , ...] , [ " class " : " ccccc " , id : 3373381 , ...]] )
	 *}</p>
	 */
	public Map addDocumentFieldItems(String index, String type, Object id, List<String> field, List<Map> items) {
		Map<String, Object> params = ["ids": items*.id, "objs": formatField(items)]
		String scriptDelete = assembleDeleteScript(field, "it.id in ids"),
			   scriptAdd = assembleAddItemScript(field, "objs", true)

		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		request.add(client.prepareUpdate(index, type, id.toString())
				.setScript(EsApiWrapper.getScript(scriptDelete, params)))
		request.add(client.prepareUpdate(index, type, id.toString())
				.setScript(EsApiWrapper.getScript(scriptAdd, params)))
		BulkResponse response = request.get()
		return [error: response.hasFailures(), message: response.buildFailureMessage()]
	}

	/**
	 * 批量给文档数组属性添加元素
	 * 示例：ctx.esIndexService.addDocumentFieldItems([
	 [ index:'boe_bpm_ct_ctvendormasterdata', type:'document', id:'3246608', field:["processInstance", "tasks"], items:[["class":"eee", id:3373384], ["class":"ffff", id:3373381]] ],
	 [ index:'boe_bpm_ct_ctvendormasterdata', type:'document', id:'3011343', field:["processInstance", "tasks"], item:["class":"eee", id:3373384] ],
	 [ index:'boe_bpm_ct_ctvendormasterdata', type:'document', id:'30113423', field:["processInstance", "tasks"], item:["class":"eee", id:3373384] ]	// 此条数据是不存在的
	 ])
	 */
	public Map addDocumentFieldItems(List<Map> list) {
		Map result = [errors: [], messages: []]
		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		Integer length = 0
		for (Map m in list) {
			Map<String, Object> params = [:]
			String deleteCondition, scriptAdd, scriptDelete
			if (m.item) {
				m.item = formatField(m.item)
				params.put("obj", m.item)
				deleteCondition = "it.id == obj.id"
				scriptAdd = assembleAddItemScript(m.field as List, "obj", false)
			} else if (m.items) {
				m.items = formatField(m.items)
				params.put("objs", m.items)
				params.put("ids", (m.items as List<Map>)*.id)
				deleteCondition = "it.id in ids"
				scriptAdd = assembleAddItemScript(m.field as List, "objs", true)
			}
			scriptDelete = assembleDeleteScript(m.field as List, deleteCondition)

			String source = new JSON(m.item ?: m.items).toString()
			if (length > 0 && length + source.length() + scriptAdd.length() + scriptDelete.length() > maxBulkSize) {
				length = 0
				BulkResponse response = request.get()
				result.errors << response.hasFailures()
				result.messages << (response.hasFailures() ? response.buildFailureMessage() : null)
			}
			length += source.length() + scriptAdd.length() + scriptDelete.length()

			request.add(client.prepareUpdate(m.index as String, m.type as String, m.id.toString())
					.setScript(EsApiWrapper.getScript(scriptDelete, params)))
			request.add(client.prepareUpdate(m.index as String, m.type as String, m.id.toString())
					.setScript(EsApiWrapper.getScript(scriptAdd, params)))
		}
		BulkResponse response = request.get()
		result.errors << response.hasFailures()
		result.messages << (response.hasFailures() ? response.buildFailureMessage() : null)
		return result
	}

	/**
	 * 生成删除属性的脚本
	 */
	private String assembleDeleteScript(List<String> path, String condition) {
		StringBuilder sb = new StringBuilder("if (")
		for (int i = 0; i < path.size(); i++) {
			if (i > 0) sb << " && "
			sb << "ctx._source.'" << path.subList(0, i + 1).join("'.'") << "'"
		}
		sb << ")"

		sb << "ctx._source"
		for (int i = 0; i < path.size(); i++) {
			sb << ".'" << path[i] << "'"
		}
		sb << ".removeAll{ " << condition << " }"
	}

	/**
	 * 生成添加属性元素的脚本
	 */
	private String assembleAddItemScript(List<String> path, String valueName, Boolean multiple) {
		// if ( !(ctx._source.'r35' instanceof List) ) ctx._source.'r35' = ['aa':objs]
		// if ( ctx._source.'r33' == null ) ctx._source.'r33' = []; ctx._source.'r33'.addAll(objs)
		StringBuilder sb = new StringBuilder()
		int len = path.size()
		for (int i = 0; i < len; i++) {
			sb << "if ( !ctx._source.'" << path.subList(0, i + 1).join("'.'") << "' ) {"
			sb << "ctx._source.'" << path.subList(0, i + 1).join("'.'") << "' = "
			if (i + 1 < len) {
				sb << "['" << path.subList(i + 1, len).join("':['") << "':"
				if (multiple) {
					sb << valueName
				} else {
					sb << "[" << valueName << "]"
				}
				sb << ("]" * (len - i - 1))
			} else if (multiple) {
				sb << valueName
			} else {
				sb << "[" << valueName << "]"
			}
			sb << "} else "
		}
		sb << "{ ctx._source.'" << path.join("'.'") << "'." << (multiple ? "addAll(" : "add(") << valueName << "); }"
		return sb.toString()
	}

	/**
	 * 更新文档属性值
	 */
	public Boolean updateDocumentField(String index, String type, Object id, String field, Object value, Object parentId = null) {
		updateDocumentField(index, type, id, [field] as List, value, parentId)
	}

	/**
	 * 更新文档某一个字段的值
	 *
	 * @param index 索引名
	 * @param type 类型名
	 * @param id 文档id
	 * @param field 要更新的字段路径，字符串或字符串列表
	 * @param value 要更新的字段的值
	 */
	public Boolean updateDocumentField(String index, String type, Object id, List<String> field, Object value, Object parentId = null) {
		XContentBuilder source = jsonBuilder().startObject()
		int len = field.size()
		for (int i = 0; i < len - 1; i++) source.startObject(field[i])
		source.field(field[len - 1], formatField(value))
		for (int i = 0; i < len; i++) source.endObject()

		UpdateRequestBuilder request = esCoreService.getClient().prepareUpdate(index.toLowerCase(), type, id.toString())
		if (parentId) request.setParent(parentId.toString())
		UpdateResponse response = request.setDoc(source).get()
		return response.shardInfo.failed == 0
	}

	/**
	 * 批量更新文档属性值
	 *
	 * <p>{@code
	 * [[index: 'foo_oa_hr_askforleave', type: 'permission', id: '1_e4', field: ['aa', 'bb'], value: '333sb', parentId: "1"],
	 *    [index: 'foo_oa_hr_askforleave', type: 'process', id: '1', field: 'definitionName', value: '3333sb', parentId: "1"],
	 *    [index: 'foo_oa_hr_askforleave', type: 'document', id: '1', field: 'tianshu', value: 20.7]]
	 *}</p>
	 * @param list 包含 index、type、id、field、value(只能是数字、字符串、集合，不能是java bean)、parentId 的 Map 列表。
	 */
	public Boolean updateDocumentFields(List<Map> list) {
		Client client = esCoreService.getClient()
		BulkRequestBuilder request = client.prepareBulk()
		for (Map m in list) {
			StringBuilder sb = new StringBuilder("ctx._source")
			if (m.field instanceof String) {
				sb << '.remove("' << m.field << '")'
			} else {
				Collection<String> fields = m.field as Collection
				int len = fields.size()
				for (int i = 0; i < len - 1; i++) sb << "." << fields[i]
				sb << '.remove("' << fields[len - 1] << '")'
			}
			request.add(client.prepareUpdate((String) m.index, (String) m.type, m.id.toString())
					.setScript(EsApiWrapper.getScript(sb.toString(), null)))

			XContentBuilder source = jsonBuilder().startObject()
			if (m.field instanceof String) {
				source.field(m.field as String, formatField(m.value))
			} else {
				Collection<String> fields = m.field as Collection
				int len = fields.size()
				for (int i = 0; i < len - 1; i++) source.startObject(fields[i])
				source.field(fields[len - 1], formatField(m.value))
				for (int i = 0; i < len - 1; i++) source.endObject()
			}
			source.endObject()

			UpdateRequestBuilder builder = client.prepareUpdate(
					m.index as String, m.type as String, m.id.toString()).setDoc(source)
			if (m.parentId) builder.setParent(m.parentId.toString())
			request.add(builder)
		}
		BulkResponse response = request.get()
		return !response.hasFailures()
	}

	/**
	 * 格式化属性,避免存到es时，属性不对应
	 * BigDecimal-》double；
	 * @param o
	 * @return
	 */
	private Object formatField(Object o) {
		if (o instanceof Collection) {
			List list = o.toArray().toList()
			for (int i = 0; i < list.size(); i++) {
				list[i] = formatField(list[i])
			}
			o = list
		} else if (o instanceof Map) {
			for (Map.Entry e in o.entrySet()) {
				e.value = formatField(e.value)
			}
		} else if (o instanceof java.sql.Timestamp) {
			return new Date(o.getTime())
		} else if (o instanceof BigDecimal) {
			return o.doubleValue()
		} else if (o instanceof String) {
			// \v 传到es后解析不了,会报错：
			// MapperParsingException[failed to parse [travels.shiyou]];
			// nested: JsonParseException[Unrecognized character escape 'v' (code 118)
			return o.replaceAll("\\\\v|\\\\a", "")
		}
		return o
	}

	/**
	 * 获得文档
	 */
	@groovy.transform.CompileStatic
	public Map<String, Object> getDocument(String index, String type, Object id, Object parentId = null) {
		Client client = esCoreService.getClient()
		GetRequestBuilder request = client.prepareGet(index, type, id.toString())
		if (parentId != null) {
			request.setParent(parentId.toString())
		}
		GetResponse response = request.setOperationThreaded(false).get()
		return response.getSource()
	}

	/**
	 * 判断文档是否存在
	 */
	@groovy.transform.CompileStatic
	public Boolean isDocumentExists(String index, String type, Object id) {
		Client client = esCoreService.getClient()
		SearchResponse response = client.prepareSearch(index).setSize(0).setTerminateAfter(1)
				.setQuery(QueryBuilders.idsQuery(type).addIds(id.toString())).get()
		return response.hits.totalHits != 0
	}


	@groovy.transform.CompileDynamic
	private test(ctx) {
		assert ctx != null
		//import org.elasticsearch.index.query.QueryBuilders
		String index = "test", type1 = "foo", type2 = "bar"

		EsIndexService s = ctx.esIndexService
		EsCoreService esCoreService = ctx.esCoreService
		EsSearchService esSearchService = ctx.esSearchService

		def client = esCoreService.getClient([reload: true])
		assert client

		// DONE 一级文档可以直接删除，但子文档无法删除，和 has_parent 查询不到有关 --- 暂时绕过
		// TODO 但是如果索引更新不及时，仍会导致子文档未删除的情况，还得清理这类垃圾数据！
		//return s.deleteDocument(index, type1, 1)

		/* -- id 查询
		return client.prepareSearch(index)
			//.setTypes(type1)
			//.setRequestCache(false)
			.setQuery(QueryBuilders.idsQuery(type2).ids("11", "12"))
			//.setQuery(org.elasticsearch.index.query.QueryBuilders.matchQuery("id", "1"))
			.get().hits.properties //*/

		/* -- DONE has_parent 查询，始终查不到 --- 暂时绕过
		return client.prepareSearch(index)
			//.setRequestCache(false)
			.setQuery(QueryBuilders.hasParentQuery(type1, QueryBuilders.idsQuery().ids("1")))
			.get().hits.properties //*/
		/*
		return client.prepareSearch(index)
			.setQuery(QueryBuilders.hasParentQuery(type1, QueryBuilders.idsQuery().ids("1")))
			.get().hits.properties //*/
		// 换成下面的条件查询是 OK 的
		/*
		return client.prepareSearch(index)
			.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("parentId", 1)).filter(QueryBuilders.typeQuery(type2)))
			.get().hits.properties*/
		//*
		//return esSearchService.searchChildren(index, type2, 1) //*/

		// 删除并重新创建索引
		s.deleteIndex(index)

		assert false == s.isIndexExists(index)
		assert s.createIndexIfNotExists(index, [defaultMapping: true])
		assert s.hasMappings(index)

		// 创建 mapping：必须先子、后父，为啥子这么奇怪？
		assert s.putMappingIfNotExists(index, type2, type1)
		assert s.putMappingIfNotExists(index, type1)

		assert s.getMappings(index).keys()*.value.sort() == ["_default_", type1, type2].sort()
		assert s.getMapping(index, type1)
		assert s.getMapping(index, type2)
		assert s.hasMapping(index, type1)
		assert s.hasMapping(index, type2)

		assert s.getChildTypes(index, type1) == [type2]

		// 索引文档
		// NOTFIX 索引后，source 里的 id 是 long 类型，而 _id 是字符串的 -- 不是问题
		assert s.indexDocuments(index, type1,
				[[id: 1], [id: 2, name: "n2", x: [y: 1], items: [[id: 21], [id: 22], [id: 23], [id: 24]]]],
				null)
		assert s.indexDocuments(index, type2,
				[[id: 11, parentId: 1], [id: 12, parentId: 1], [id: 13, parentId: 1], [id: 21, parentId: 2]],
				[parent: "parentId", idGetter: { doc -> doc.id }])

		assert esSearchService.searchChildren(index, type1, 1)
		assert s.getDocument(index, type1, 1)?.id == 1
		assert s.getDocument(index, type1, 2)?.id == 2
		assert s.getDocument(index, type2, 11, 1)?.id == 11
		assert s.getDocument(index, type2, 12, 1)?.id == 12
		assert s.getDocument(index, type2, 13, 1)?.id == 13
		assert s.getDocument(index, type2, 21, 2)?.id == 21

		//return "创建索引后退出！"

		/* -- id 查询，必须稍等片刻或者刷新后才能查到
		s.refresh(index)
		return client.prepareSearch(index)
			//.setTypes(type1)
			//.setRequestCache(false)
			.setQuery(org.elasticsearch.index.query.QueryBuilders.idsQuery(type1).ids("1"))
			.get().hits.properties
		//*/

		// 删除文档
		// 必须 sleep 或者 refresh 等待索引创建完成，否则删不干净
		s.refresh(index)
		assert false == s.deleteDocument(index, type2, 13)
		assert s.deleteDocument(index, type2, 13, 1)
		assert !s.getDocument(index, type2, 13, 1)

		assert s.deleteDocument(index, type1, 1)
		assert !s.getDocument(index, type1, 1)
		assert !s.getDocument(index, type2, 11, 1)

		// 删除属性：需要配置es，允许执行脚本，如：
		// script.inline: true
		// script.indexed: true
		// 详见 https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting.html
		// curl -XPOST 'localhost:9200/test/foo/2/_update' -d '{"script" : "ctx._source.remove(\"name\")"}'
		assert s.getDocument(index, type1, 2).containsKey("name")
		assert s.deleteDocumentField(index, type1, 2, "name")
		assert !s.getDocument(index, type1, 2).containsKey("name")

		assert s.getDocument(index, type1, 2).x.containsKey("y")
		assert s.deleteDocumentField(index, type1, 2, ["x", "y"])
		assert !s.getDocument(index, type1, 2).x.containsKey("y")

		//curl -XPOST 'localhost:9200/test/foo/2/_update' -d '{"script" : "if (ctx._source.'items')ctx._source.'items'.removeAll{ it.id in ['1','2'] }"}'
		assert s.deleteDocumentFieldItems(index, type1, 2, ["items"], [23, 24]).error == false
		assert s.getDocument(index, type1, 2).items.size() == 2

		assert s.addDocumentFieldItem(index, type1, 2, ["items"], [id: 25]).error == false
		assert s.getDocument(index, type1, 2).items.size() == 3
		assert s.getDocument(index, type1, 2).items[2] == [id: 25]

		assert s.addDocumentFieldItem(index, type1, 2, ["items"], [id: 25, x: 1]).error == false
		assert s.getDocument(index, type1, 2).items.size() == 3
		assert s.getDocument(index, type1, 2).items[2] == [id: 25, x: 1]

		assert s.addDocumentFieldItems(index, type1, 2, ["items"], [[id: 21, x: 1], [id: 22, x: 1]]).error == false
		assert s.getDocument(index, type1, 2).items.size() == 3
		assert s.getDocument(index, type1, 2).items[2].id == 22 // 先删除，再添加，所以 22 变成最后一条了

		assert s.addDocumentFieldItems([
				[index: index, type: type1, id: 2, field: ["items"], items: [[id: 21, x: 2], [id: 26]]],
				[index: index, type: type1, id: 2, field: ["items"], item: [id: 27]]
		]).errors.unique() == [false]
		assert s.getDocument(index, type1, 2).items.size() == 5

		assert s.updateDocumentField(index, type1, 2, "name", "nn22")
		assert s.getDocument(index, type1, 2).name == "nn22"

		assert s.updateDocumentFields([[index: index, type: type1, id: 2, field: "xxx", value: "yyy"]])
		assert s.getDocument(index, type1, 2).xxx == "yyy"

		assert s.updateDocumentField(index, type2, 21, "name", "n11", 2)
		assert s.getDocument(index, type2, 21, 2).name == "n11"

		// 按照类型删除文档，全部异常 -- 服务端没装插件，必须 sleep 再删，删后还得 sleep 再查
		s.refresh(index)
		s.deleteDocuments(index, type1)
		s.refresh(index)
		assert s.hasMapping(index, type1)
		assert esSearchService.search([query: QueryBuilders.typeQuery(type1)]).total == 0

		s.deleteDocuments(index, type2)
		s.refresh(index)
		assert s.hasMapping(index, type2)
		assert esSearchService.search([query: QueryBuilders.typeQuery(type2)]).total == 0

		// 删除索引
		//assert s.deleteIndex(index)
		return "passed!"
	}

}