<%@ page import="bropen.toolkit.utils.grails.DbUtils" %>
<%@ page import="bropen.toolkit.utils.StringUtils" %>
<%@ page import="bropen.framework.core.DataDict" %>
<%@ page import="bropen.framework.core.DataDictData" %>
<%@ page import="bropen.search.config.Searchable" %>
<%@ page import="bropen.search.config.SearchableField" %>
<g:if test="${actionName == "getDomainProperties"}">
	<meta name="layout" content="blank" />
</g:if>

<tr class="prop">
	<td class="name"><label for="disabled">
		<g:message code="bropen.search.config.Searchable.disabled" default="Disabled"/></label></td>
	<td class="value">
		<o:field bean="${searchableInstance}" name="disabled" editable="${editable}">
			<g:checkBox name="disabled" value="${searchableInstance.disabled}"/>
		</o:field>
	</td>
	<td class="name"><label for="unclassified">
		<g:message code="bropen.search.config.Searchable.unclassified" default="Unclassified"/></label></td>
	<td class="value">
		<o:field bean="${searchableInstance}" name="unclassified" editable="${editable}">
			<g:checkBox name="unclassified" value="${searchableInstance.unclassified}"/>
		</o:field>
	</td>
</tr>

<tr class="prop">
	<td class="name"><label for="categories">
		<g:message code="bropen.search.config.Searchable.categories" default="Disabled"/></label></td>
	<td class="value" colspan="3">
		<% List categories = searchableInstance.categories?.tokenize(";") %>
		<g:if test="${editable}">
			<g:each in="${Searchable.getAllCategories([main: true])}" var="data">
				<input type="checkbox" name="categories" value="${data.id}"
					${categories?.contains(data.id.toString()) ? 'checked=""' : ''} />${data.name}&nbsp;&nbsp;
			</g:each>
		</g:if><g:elseif test="${categories}">
			${(searchableInstance as Searchable).getCategoryNames().join("、")}
		</g:elseif>
	</td>
</tr>

<tr class="prop">
	<td class="name"><label for="title">
		<g:message code="bropen.search.config.Searchable.title" default="Title"/></label></td>
	<td class="value" colspan="3">
		<o:field bean="${searchableInstance}" name="title" editable="${editable}" class="textarea">
			<g:textArea name="title" value="${searchableInstance.title}"/>
		</o:field>
	</td>
</tr>

<tr class="prop">
	<td class="name"><label for="summary">
		<g:message code="bropen.search.config.Searchable.summary" default="Summary"/></label></td>
	<td class="value" colspan="3">
		<o:field bean="${searchableInstance}" name="summary" editable="${editable}" class="textarea">
			<g:textArea name="summary" value="${searchableInstance.summary}"/>
		</o:field>
	</td>
</tr>

<tr class="prop">
	<td class="name"><label for="index">
		<g:message code="bropen.search.config.Searchable.index" default="Index"/></label></td>
	<td class="value">
		<o:field bean="${searchableInstance}" name="index" editable="${!searchableInstance.id}">
			<g:textField name="index" maxlength="255" required="" value="${searchableInstance.index}" class="middle"/>
		</o:field>
	</td>
	<td class="name"><label for="url">
		<g:message code="bropen.search.config.Searchable.url" default="Url"/></label></td>
	<td class="value">
		<o:field bean="${searchableInstance}" name="url" editable="${!searchableInstance.id}">
			<g:textField name="url" maxlength="255" required="" value="${searchableInstance.url}" class="middle"/>
		</o:field>
	</td>
</tr>

<tr class="prop">
	<g:if test="${searchableInstance.docClass}">
		<td class="value o-inner-table" colspan="4" style="padding: 0px">
			<% Map<String, Map<String, List<Map>>> propsMap = (searchableInstance as Searchable).getDomainProperties() %>
			<table>
				<thead>
				<tr>
					<th><g:message code="bropen.search.config.SearchableField.name" default="Name"/></th>
					<th><g:message code="bropen.search.config.SearchableField.name.field" default="Field"/></th>
					<th><g:message code="bropen.search.config.SearchableField.child" default="Child"/></th>
					<th width="60%"><g:message code="bropen.search.config.SearchableField.mapping"
											   default="Mapping"/></th>
					<th><g:message code="bropen.search.config.Searchable.disabled" default="Disabled"/></th>
				</tr>
				</thead>
				<tbody>
				<g:each in="${propsMap.keySet()}" var="fieldPath">
					<g:each in="${['ordinaries', 'excludes', 'mappingSqls', 'mappings', 'parent', 'children']}" var="key">
						<% Map properties = propsMap.get(fieldPath) %>
						<g:each in="${properties.get(key)}" var="p">
							<%
								// 主类的 id 不显示
								if (fieldPath == searchableInstance.docClass && p.name == "id") continue
								// 拼接显示用内容
								String fieldPath2 = fieldPath
								if (fieldPath == searchableInstance.docClass) fieldPath2 = ""
								String fieldName = fieldPath2 + (fieldPath2 ? "." : "") + p.name
								String displayName = (Collection.isAssignableFrom(p.type) ? "Collection" : Map.isAssignableFrom(p.type) ? "Map" : "")
								if (p.isDomain) displayName += (displayName ? "<" : "") + p.reftype.simpleName + (displayName ? ">" : "")
								displayName += " " + fieldName
								String mappings = key == 'mappings' ?
										(p.mappingProps ? StringUtils.toJson(p.mappingProps) : null) :
										(key == 'mappingSqls' ? p.mappingSql : null)
							%>
							<tr>
								<td>
									<g:message code="${properties.clazz.name}.${p.name}" default=""/>
									<g:hiddenField name="field.docClass" value="${properties.clazz.name}"/>
								</td>
								<td>
									${displayName.encodeAsHTML()}
									<g:hiddenField name="field.name" value="${fieldName}"/>
								</td>
								<td>
									<g:if test="${editable}">
										<g:if test="${p.isDomain}">
											<input type="checkbox" name="field._child" ${p.child == 1 ? 'checked=""' : ''}/>
											<input type="hidden" name="field.child" value="${p.child == 1}"/>
										</g:if>
										<g:else>
											<input type="hidden" name="field.child" value="false"/>
										</g:else>
									</g:if>
									<g:else>
										${key == 'children' && p.child == 1 ? message(code: 'default.boolean.true') : null}
									</g:else>
								</td>
								<td>
									<g:if test="${editable}">
										<g:if test="${(p.isDomain || Collection.isAssignableFrom(p.type) || Map.isAssignableFrom(p.type))}">
											<g:textArea name="field.mappings" value="${mappings}" style="height: 30px"/>
										</g:if>
										<g:else>
											<g:hiddenField name="field.mappings" value=""/>
										</g:else>
									</g:if>
									<g:else>
										${key == 'mappings' ? (p.mappingProps ? StringUtils.toJson(p.mappingProps) : null) : (key == 'mappingSqls' ? p.mappingSql : null)}
									</g:else>
								</td>
								<td>
									<g:if test="${editable}">
										<input type="checkbox" name="field._disabled" ${key == 'excludes' ? 'checked=""' : ''}/>
										<input type="hidden" name="field.disabled" value="${key == 'excludes'}"/>
									</g:if>
									<g:else>
										${key == 'excludes' ? message(code: 'default.boolean.true') : null}
									</g:else>
								</td>
							</tr>
						</g:each>
					</g:each>
				</g:each>
				</tbody>
			</table>
		</td>
	</g:if>
</tr>

<tr class="prop">
	<td class="name"><label for="notes">
		<g:message code="bropen.search.config.Searchable.notes" default="Notes"/></label></td>
	<td class="value" colspan="3">
		<o:field bean="${searchableInstance}" name="notes" editable="${editable}" class="textarea">
			<g:textArea name="notes" maxlength="500" value="${searchableInstance.notes}"/>
		</o:field>
	</td>
</tr>

<tr class="prop">
	<td class="name"><g:message code="default.createdBy" default="Created By"/></td>
	<td class="value">${searchableInstance.createdBy}</td>
	<td class="name"><g:message code="default.updatedBy" default="Updated By"/></td>
	<td class="value">${searchableInstance.updatedBy}</td>
</tr>

<tr class="prop">
	<td class="name"><g:message code="default.dateCreated" default="Date Created"/></td>
	<td class="value"><g:formatDate format="${searchableInstance.constrainedProperties.dateCreated?.format}" date="${searchableInstance.dateCreated}"/></td>
	<td class="name"><g:message code="default.lastUpdated" default="Last Updated"/></td>
	<td class="value"><g:formatDate format="${searchableInstance.constrainedProperties.lastUpdated?.format}" date="${searchableInstance.lastUpdated}"/></td>
</tr>