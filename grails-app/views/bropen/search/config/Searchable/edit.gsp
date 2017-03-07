<%@ page import="bropen.search.config.Searchable" %>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<meta name="layout" content="${grailsApplication.config.bropen.ui.layout.form}" />
<g:set var="entityName" value="${message(code: 'bropen.search.config.Searchable', default: 'Searchable')}" />
<title><g:message code="default.edit.label" args="[entityName]" /> </title>
</head>

<body>
<div class="body">
	<h1>${entityName}</h1>
	<o:renderFlashMessages />
	<o:renderFieldErrors bean="${searchableInstance}"/>
	<g:form method="post">
		<g:hiddenField name="id" value="${searchableInstance.id}"/>
		<g:hiddenField name="version" value="${searchableInstance.version}"/>
		<div class="dialog" style="min-width: 1200px">
			<table class="column2">
				<g:render template="/bropen/search/config/Searchable/form"/>
			</table>
		</div>

		<div class="buttons">
			<o:button builtin="update" />
			<sec:permit action='delete'>
				<o:button builtin="delete" onclick="return confirm('${message(code: 'default.button.delete.confirm.message')}') && showProcessingDlg()"/>
			</sec:permit>
		</div>
	</g:form>
</div>
</body>
</html>
