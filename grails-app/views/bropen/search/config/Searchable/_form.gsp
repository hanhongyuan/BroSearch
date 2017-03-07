<%@ page import="bropen.toolkit.utils.StringUtils" %>
<%@ page import="bropen.toolkit.utils.grails.BeanUtils" %>
<%@ page import="bropen.toolkit.utils.grails.GrailsUtils" %>
<%@ page import="bropen.framework.core.security.DomainApplication" %>
<%@ page import="bropen.search.config.Searchable" %>
<%@ page import="bropen.search.config.SearchableField" %>

<script type="text/javascript">
	/**
	 * 根据所选应用加载 Domain 类列表
	 */
	function loadClasses() {
		if (getServerHost()) {
			var url = getServerHost() + "${createLink(action: "getDomainClasses")}"
			$.ajaxSetup({cache: false})
			$("td.docClass").load(url, function () {
				$("#fields").html("")
			})
		} else {
			$("td.docClass").html("")
		}
	}

	/**
	 * 根据所选 Domain 类，加载属性列表
	 */
	function loadFields(editable) {
		var docClass = $("#docClass").val()
		if (docClass) {
			var url = getServerHost() + "${createLink(action: "getDomainProperties")}"
			editable = (typeof(editable) == "object" ? true : editable)
			$("#fields").load(url, {docClass: docClass, editable: editable})
		} else {
			$("#fields").html("")
		}
	}

	/**
	 * 获取服务器地址
	 */
	function getServerHost() {
		var url,
			$app = $("#application\\.id")
		if ($app.prop("tagName") == "SELECT") {
			url = $app.find("> :selected").attr("url")
		} else if ($app.length) {
			url = $app.val()
		} else {
			url = "${(searchableInstance as Searchable).application?.serverURL}"
		}
		return url ? url.replace(/\/[^\/]+\/$/, "") : "" // 去掉上下文根，仅保留协议、主机、端口
	}

	/**
	 * 校验 mappingProps、mappingSql
	 */
	function validateMappings() {
		if ($("#field\\.name").length == 0) return true
		var success = false
		showProcessingDlg()
		var data = {
			docClass: $("#docClass").val(),
			names: $("[name='field\\.name']").map(function () {
				return $(this).val()
			}).get(),
			mappings: $("[name='field\\.mappings']").map(function () {
				return $(this).val()
			}).get()
		}
		$.ajaxSetup({async: false})
		$.post(getServerHost() + "${createLink(action: "validateMappings")}", {data: JSON.stringify(data)}, function (data) {
			if (data.success) {
				success = true
			} else {
				hideProcessingDlg()
				$(".user-error[name='field\\.mappings']").removeClass("user-error")
				success = false
				var errors = data.errors
				for (var i = 0; i < errors.length; i++) {
					var error = errors[i]
					$("[name='field\\." + error.name + "']:eq(" + error.index + ")").addClass("user-error").qtip({
						content: error.error
					})
				}
			}
		})
		return success
	}

	/**
	 * 校验 title、summary 格式是否正确
	 */
	function validateAdditionalProps() {
		var success = true
		var arr = [$("#title"), $("#summary")]
		for (var i=0; i<arr.length; i++) {
			var $e = arr[i]
			if ($e.val()) {
				try {
					JSON.parse($e.val())
					$e.removeClass("user-error")
				} catch (E) {
					$e.addClass("user-error").qtip({
						content: "不是json格式"
					})
					success = false
				}
			}
		}
		return success
	}

	$(function () {
		$(document)
			.on("change", "#application\\.id", loadClasses)
			.on("change", "#docClass", loadFields)
			.on("change", "[name^='field._']:checkbox", function () {
				$(this).closest("td").find("[name^='field.']:hidden").val(this.checked)
			})

		// 保存前进行表单校验
		$(":submit.save, :submit.update").on("click", function (event) {
			if (validateAdditionalProps() && validateMappings()) {
				var $form = $("form")
				$form.attr("action", getServerHost() + $form.attr("action").replace(/^http:\/\/.+?\//, '/'))
			} else {
				hideProcessingDlg()
				event.preventDefault()
			}
		})
	})
</script>

<g:hiddenField name="custom" value="true"/>

<tbody>
<tr class="prop">
	<td class="name"><label for="application">
		<g:message code="bropen.search.config.Searchable.application" default="Application"/></label></td>
	<td class="value" colspan="3">
		<o:field bean="${searchableInstance}" name="application" editable="${!searchableInstance.id}"
					 value="${searchableInstance.application?.fullName}">
			<sec:applicationSelect
					name="application.id" optionValue="fullName" optionUrl="serverURL"
					value="${searchableInstance.application?.id}"/>
		</o:field>
	</td>
</tr>

<tr class="prop">
	<td class="name"><label for="docClass">
		<g:message code="bropen.search.config.Searchable.docClass" default="Doc Class"/></label></td>
	<td class="value docClass" colspan="3">
		<g:if test="${searchableInstance.id}">
			${searchableInstance.docClass}
			<g:hiddenField name="docClass" value="${searchableInstance.docClass}"/>
		</g:if>%{--<g:else>
			<g:render template="/bropen/search/config/Searchable/form_docClass"/>
		</g:else>--}%
	</td>
</tr>
</tbody>

<tbody id="fields">
<% boolean editable = o.isFieldEditable(name: "fields") %>
<g:if test="${searchableInstance.applicationId == DomainApplication.current().id}">
	<g:render template="/bropen/search/config/Searchable/form_props"
			  model="[searchableInstance: searchableInstance, editable: editable]"/>
</g:if><g:else>
	<script>
		loadFields(${editable})
	</script>
</g:else>
</tbody>