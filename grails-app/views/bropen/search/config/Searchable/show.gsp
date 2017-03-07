<%@ page import="bropen.search.config.Searchable" %>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<meta name="layout" content="${grailsApplication.config.bropen.ui.layout.form}" />
<g:set var="entityName" value="${message(code: 'bropen.search.config.Searchable', default: 'Searchable')}" />
<title><g:message code="default.show.label" args="[entityName]" /> </title>
</head>

<body>
<sec:permit action='show'><div class="body">
	<h1>${entityName}</h1>
	<o:renderFlashMessages />

	<div class="dialog" style="min-width: 1200px">
		<table class="column2">
			<g:render template="/bropen/search/config/Searchable/form" />
		</table>
	</div>

	<div class="buttons">
		<g:form>
			<g:hiddenField name="id" value="${searchableInstance.id}"/>
			<sec:permit action='edit'>
				<o:button builtin="edit"/>
				<o:button builtin="disable" test="${!searchableInstance.disabled}"
						  onclick="return confirm('${message(code: 'default.button.disable.confirm.message')}') && showProcessingDlg()"/>
				<o:button builtin="enable" test="${searchableInstance.disabled}"
						  onclick="return confirm('${message(code: 'default.button.enable.confirm.message')}') && showProcessingDlg()"/>
				<o:button builtin="reset" test="${searchableInstance.custom}"
						  onclick="return confirm('${message(code: 'default.button.reset.confirm.message')}') && showProcessingDlg()"/>
			</sec:permit>
			<sec:permit action='delete'>
				<o:button builtin="delete" onclick="return confirm('${message(code: 'default.button.delete.confirm.message')}') && showProcessingDlg()"/>
			</sec:permit>
		</g:form>
	</div>
</div></sec:permit>
</body>
</html>
