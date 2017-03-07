package bropen.search.config

import bropen.toolkit.utils.StringUtils

/**
 * 属性索引
 */
class SearchableField {

	static belongsTo = [searchable: Searchable]

	/** 所属文档类 */
	String docClass

	/** 属性名，可为"aa.bb" */
	String name

	/** 是否禁用 */
	Boolean disabled = Boolean.FALSE

	/**
	 * 引用类是否是子文档
	 */
	Boolean child = Boolean.FALSE

	/** 自定义查询字段 */
	String mappingProps

	/** 自定义查询sql */
	String mappingSql

	///////////////////////////////////////////////////////////////////////////

	static constraints = {
		searchable(nullable: false)
		docClass(nullable: true)
		name(nullable: false, blank: false)
		disabled(nullable: false)
		child(nullable: false)
		mappingProps(nullable: true, maxSize: 500)
		mappingSql(nullable: true, maxSize: 2000)
	}

	static mapping = {
		version false
		table "brosea_searchable_field"
	}

}
