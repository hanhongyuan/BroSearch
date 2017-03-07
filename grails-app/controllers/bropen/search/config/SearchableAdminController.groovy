package bropen.search.config

import bropen.framework.Constants
import bropen.framework.core.security.DomainApplication
import bropen.framework.traits.ScaffoldController
import bropen.search.es.EsIndexService
import bropen.search.orm.HibernateEventListener
import bropen.toolkit.utils.StringUtils
import bropen.toolkit.utils.grails.BeanUtils
import bropen.toolkit.utils.grails.DbUtils

import grails.transaction.Transactional
import grails.converters.JSON

@Transactional(readOnly = true)
class SearchableAdminController implements ScaffoldController {

	static domainSuffix = "Instance"

	static scaffold = Searchable

	static allowedMethods = [save: "POST", update: "POST", delete: "POST"]

	SearchableService searchableService

	///////////////////////////////////////////////////////////////////////////

	/**
	 * AJAX：create 时，获取并渲染Domain类的属性列表
	 */
	def getDomainProperties(String docClass, Boolean editable) {
		assert docClass
		Searchable searchable = new Searchable()
		searchable.properties = params
		searchable.index = EsIndexService.getIndexName(searchable.docClass)
		searchable.url = searchableService.generateUrl(Class.forName(searchable.docClass))
		if (!editable) request.setAttribute(Constants.REQ_FORM_RO, true)
		render(view: "${VIEW_PATH}_form_props", model: [searchableInstance: searchable, editable: editable])
	}

	/**
	 * AJAX：create 时，获取并渲染未配置的Domain类列表
	 */
	def getDomainClasses() {
		render(view: "${VIEW_PATH}_form_docClass")
	}

	@Transactional
	def save() {
		withInstance({ Searchable searchableInstance, status ->
			searchableInstance.properties = params
			searchableInstance.categories = params.categories ? ";${params.list("categories").join(";")};" : null

			assambleFields(searchableInstance)

			//updateDynamicTable(searchableInstance, SearchableItem, "items", "XXXX", params, "item", "XXXX")	// 手动保存动态表
			//updateDynamicTables(searchableInstance, params)	// 自动保存所有动态表
			if (!searchableInstance.hasErrors() && saveReferences(searchableInstance) && searchableInstance.save(flush: true)) {

				disableSearchable(searchableInstance, false)

				//updateAttachments(searchableInstance)	// 保存附件
				if (isAjax()) {
					if (params.id)
						renderAjaxMessage([message: message(code: 'default.updated.message', args: [message(code: c1, default: c2), searchableInstance]), id: searchableInstance.id])
					else
						renderAjaxMessage([message: message(code: 'default.created.message', args: [message(code: c1, default: c2), searchableInstance]), id: searchableInstance.id])
				} else {
					flash.message = message(code: 'default.created.message', args: [message(code: c1, default: c2), searchableInstance])
					redirect(action: "show", id: searchableInstance.id)
				}
			} else {
				if (isAjax()) renderAjaxError(searchableInstance)
				else render(view: "${VIEW_PATH}create", model: [searchableInstance: searchableInstance])
			}
		})
	}

	@Transactional
	def update() {
		withInstance({ Searchable searchableInstance, status ->
			searchableInstance.properties = params
			searchableInstance.categories = params.categories ? ";${params.list("categories").join(";")};" : null

			assambleFields(searchableInstance)

			//updateDynamicTable(searchableInstance, SearchableItem, "items", "XXXX", params, "item", "XXXX")	// 手动保存动态表
			//updateDynamicTables(searchableInstance, params)	// 自动保存所有动态表
			if (!searchableInstance.hasErrors() && saveReferences(searchableInstance) && searchableInstance.save(flush: true)) {

				disableSearchable(searchableInstance, false)

				//updateAttachments(searchableInstance)	// 保存附件
				if (isAjax()) {
					renderAjaxMessage([message: message(code: 'default.updated.message', args: [message(code: c1, default: c2), searchableInstance])])
				} else {
					flash.message = message(code: 'default.updated.message', args: [message(code: c1, default: c2), searchableInstance])
					redirect(action: "show", id: searchableInstance.id)
				}
			} else {
				if (isAjax()) renderAjaxError(searchableInstance)
				else render(view: "${VIEW_PATH}edit", model: [searchableInstance: searchableInstance])
			}
		})
	}

	@Transactional
	def delete() {
		withInstance({ Searchable searchableInstance, status ->
			searchableInstance.delete(flush: true)

			disableSearchable(searchableInstance, true)

			if (isAjax()) {
				renderAjaxMessage([message: message(code: 'default.deleted.message', args: [message(code: c1, default: c2), searchableInstance])])
			} else {
				flash.message = message(code: 'default.deleted.message', args: [message(code: c1, default: c2), searchableInstance])
				renderClose([reloadopener: true, message: flash.message])
			}
		})
	}

	/**
	 * 根据表单提交的数据，拼装 SearchableField
	 */
	private void assambleFields(Searchable searchable) {
		Class clazz = Class.forName(searchable.docClass)
		Map<String, Map> propsAnn = searchableService.getDomainProperties(clazz)
		List<SearchableField> fields = searchableService.generateFields(null, clazz, null, propsAnn)
		Map<String, List<SearchableField>> fieldsAnn = fields.groupBy { it.name },
									 fieldsCfg = searchable.fields?.groupBy { it.name }

		List<String> names = params.list("field.name")
		List<String> disableds = params.list("field.disabled")
		List<String> children = params.list("field.child")
		List<String> mappings = params.list("field.mappings")
		List<String> docClasses = params.list("field.docClass")
		Integer count = names.size()

		for (int i = 0; i < count; i++) {
			String name = names[i]
			Boolean disabled = StringUtils.means(disableds[i], false),
					child = StringUtils.means(children[i], false),
					isMappingProps = mappings[i]?.trim()?.startsWith("[")
			String mappingProps = isMappingProps ? mappings[i]?.trim() : null,
				   mappingSql = isMappingProps ? null : mappings[i]?.trim()
			SearchableField fieldAnn = fieldsAnn.remove(name)?.get(0),
							fieldCfg = fieldsCfg?.remove(name)?.get(0)
			Map props = [disabled: disabled, child: child, mappingProps: mappingProps, mappingSql: mappingSql]
			Boolean needSave = (disabled || child || mappingProps || mappingSql),      // 是否需要持久化
					eqAnn = (!needSave && !fieldAnn) || fieldEquals(fieldAnn, props),  // 是否和注解解析结果一致
					eqCfg = (!needSave && !fieldCfg) || fieldEquals(fieldCfg, props)   // 是否和已持久化的 SearchableField 一致

			if (needSave || !eqAnn) {
				if (fieldCfg && !eqCfg) {
					fieldCfg.properties = props
				} else if (!fieldCfg) {
					SearchableField field = new SearchableField(name: name, searchable: searchable, docClass: docClasses[i])
					field.properties = props
					searchable.addToFields(field)
				}
			} else if (fieldCfg && eqAnn && !eqCfg) {
				searchable.removeFromFields(fieldCfg)
				fieldCfg.delete()
			}
		}
	}

	private boolean fieldEquals(SearchableField field, Map props) {
		field && field.disabled == props.disabled && field.child == props.child &&
				field.mappingProps == props.mappingProps && field.mappingSql == props.mappingSql
	}

	/**
	 * 禁用
	 */
	@Transactional
	def disable(String id) {
		disableSearchable(id, true)
	}

	/**
	 * 启用
	 */
	@Transactional
	def enable(String id) {
		disableSearchable(id, false)
	}

	private void disableSearchable(String id, boolean disabled) {
		Searchable searchable = Searchable.get(id)
		if (searchable.disabled != disabled) {
			searchable.disabled = disabled
			searchable.save()
		}
		disableSearchable(searchable, disabled)
		flash.message = message(code: "default.action.success")
		redirect(action: "show", id: id)
	}

	private void disableSearchable(Searchable searchable, boolean deleted) {
		if (searchable.applicationId == DomainApplication.current().id) {
			Class clazz = BeanUtils.loadClass(searchable.docClass)
			if (deleted || searchable.disabled) {
				HibernateEventListener.removeSearchableClass(clazz)
				//EsSearchService.removeDocumentIndex(searchable.index)
			} else {
				HibernateEventListener.addSearchableClass(clazz)
				//EsSearchService.addDocumentIndex(searchable.index)
			}
		} else {
			// 跨应用：在 EsSearchService、HibernateEventListener 中定时自动更新
		}
	}

	/**
	 * 重置
	 */
	@Transactional
	def reset() {
		Searchable searchable = Searchable.get(params.id)
		searchableService.resetSearchable(searchable)
		searchable.save()
		disableSearchable(searchable, false)
		flash.message = message(code: "default.action.success")
		redirect(action: "show", id: searchable.id)
	}

	/**
	 * AJAX：校验mapping、mappingSql
	 */
	def validateMappings() {
		if (params.data) {
			Map data = JSON.parse(params.data)
			String docClass = data.docClass
			String[] names = data.names,
					 mappings = data.mappings
			List<String> errors = []
			for (int i = 0; i < names.length; i++) {
				String name = names[i],
					   mapping = mappings[i]?.trim()
				if (!mapping) continue
				// mapingProps
				if (mapping.startsWith("[")) {
					String[] mappingArr
					try {
						mappingArr = JSON.parse(mapping)
					} catch (Exception e) {
						errors << [index: i, error: "Invalid json format: ${mapping}", name: "mappings"]
						continue
					}
					String[] arr = name.split("\\.")
					Class cla = Class.forName(docClass)
					Map props
					for (int j = 0; j < arr.length; j++) {
						props = BeanUtils.getPersistentProperties(cla)
						cla = props[arr[j]].reftype  // 逐级找引用的domain类
					}
					for (int j = 0; j < mappingArr.length; j++) {
						arr = mappingArr[j].split("@") // 切割 alias@propertyName
						if (arr.length == 2) {
							arr = arr[1].split("\\.")  // 切割形如 foo.bar.xxx 的引用类属性，其中 foo 属性也必须为 Domain 类
							Class cl = cla
							for (int k = 0; k < arr.length; k++) {
								props = BeanUtils.getPersistentProperties(cl)
								if (props.containsKey(arr[k])) {
									if (props[arr[k]].isDomain || k == arr.length - 1) {
										cl = props[arr[k]].reftype
									} else if (k < arr.length - 1) {
										errors << [index: i, error: "Not a domain bean：${arr[k]}", name: "mappings"]
										break
									}
								} else {
									errors << [index: i, error: "Property dosn't exists：${arr[k]}", name: "mappings"]
									break
								}
							}
						} else {
							errors << [index: i, error: "Invalid mapping [${mappingArr[j]}], should be [\"alias@propertyName\", ...]", name: "mappings"]
							break
						}
					}
				}
				// mappingSql
				else {
					try {
						DbUtils.getCurrentSql(DbUtils.getDataSourceNameByDomain(docClass)) { sql ->
							sql.rows(mapping, 0, 1)
						}
					} catch (Exception e) {
						errors << [index: i, error: "Sql execute error!", name: "mappings"]
					}
				}
			}

			if (errors) {
				renderAjaxMessage([success: false, errors: new JSON(errors).toString()])
			} else {
				renderAjaxMessage([success: true])
			}
		}
	}

}