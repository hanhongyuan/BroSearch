<%@ page import="bropen.search.config.Searchable" %>
<g:if test="${actionName == "getDomainClasses"}">
	<meta name="layout" content="blank" />
</g:if>
<%
	// 获得所有 Domain 类
	List<String> clazzNames = Searchable.getDomainClasses()
	clazzNames -= Searchable.getSearchableClazzes([all: false, disabled: null, cache: false])*.docClass
%>
<g:select name="docClass" from="${clazzNames}" noSelection="['': '--']"/>
<script>$("#docClass").combobox()</script>