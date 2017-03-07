<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<meta name="layout" content="${grailsApplication.config.bropen.ui.layout.list}" />
<g:set var="entityName" value="${message(code: 'bropen.search.config.Searchable', default: 'Searchable')}" />
<title><g:message code="default.list.label" args="[entityName]" /> </title>
</head>

<body>
<s:form><div class="body">
	<section class="content-header">
		<ol class="breadcrumb">
			<li><i class="fa fa-angle-double-right"></i><g:message code="admin.config" default="Configuration"/></li>
			<li class="active">${entityName}</li>
		</ol>
	</section>
	<o:renderFlashMessages />
	<div class="box">
		<s:toolbar>
			<div class="left">
				<sec:permit action='create'>
					<o:button builtin="create" />
				</sec:permit>
			</div>
			<div class="right">
				<g:message code="bropen.search.config.Searchable.docClass"/>: <s:textField name="d.docClass" style="width:100px" />
				<s:submitButton />
			</div>
		</s:toolbar>

		<div class="list">
			<table>
				<thead>
					<tr>
						<th><g:message code="bropen.search.config.Searchable.application" default="Application" /></th>
						<g:sortableColumn property="docClass" title="${message(code: 'bropen.search.config.Searchable.docClass', default: 'Doc Class')}" />
						<g:sortableColumn property="index" title="${message(code: 'bropen.search.config.Searchable.index', default: 'Index')}" />
						<g:sortableColumn property="disabled" title="${message(code: 'bropen.search.config.Searchable.disabled', default: 'Disabled')}" />
						<g:sortableColumn property="custom" title="${message(code: 'bropen.search.config.Searchable.custom', default: 'Custom')}" />
					</tr>
				</thead>
				<tbody><sec:permit action='show'>
				<g:each in="${searchableInstanceList}" var="entity">
					<% String href = new StringBuilder("show/").append(entity.id).toString() %>
					<tr>
						<td><a href="${href}" target="_blank">${entity.application}</a></td>
						<td><a href="${href}" target="_blank">${entity.docClass}</a></td>
						<td><a href="${href}" target="_blank">${entity.index}</a></td>
						<td><a href="${href}" target="_blank"><g:formatBoolean boolean="${entity.disabled}" /></a></td>
						<td><a href="${href}" target="_blank"><g:formatBoolean boolean="${entity.custom}" /></a></td>
					</tr>
				</g:each>
				</sec:permit></tbody>
			</table>
		</div>
		<div class="pagination">
			<o:paginate total="${total}"/>
		</div>
	</div>
</div></s:form>
</body>
</html>
