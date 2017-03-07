<%@ page import="bropen.toolkit.utils.DateUtils" %>
<%@ page import="bropen.toolkit.utils.StringUtils" %>
<%@ page import="bropen.framework.core.DataDictData" %>
<%@ page import="bropen.search.Constants" %>
<%@ page import="bropen.search.config.Searchable" %>
<!DOCTYPE html>
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
	<meta name="layout" content="${grailsApplication.config.bropen.ui.layout.list}"/>
	<g:set var="entityName" value="搜索引擎"/>
	<title>搜索列表</title>
	<asset:stylesheet src="search.scss"/>
	<script type="text/javascript">
		$(function () {
			var $form = $("form");
			// 选择分类
			$(document).on("click", ".category li a", function () {
				$(this).closest(".category").find(".keyboard").val($(this).data("key"))
				$form.submit()
			}).on("click", ".selected li a", function () {
				// 去掉单个已选分类
				$(this).siblings(":hidden").val("")
				$form.submit()
			}).on("click", ".selected [data-widget='remove']", keyboardRequest).// 去掉所有已选分类
			on("click", ".o-search-submit", keyboardRequest).// 点击查询按钮
			on("keydown", "[name='q']", function (e) {
				// 输入框回车
				if (e.keyCode == 13)
					keyboardRequest()
			}).on("click", ".type li a", function () {
				// 选择搜索结果
				$(this).closest(".type").find(".keyboard").val($(this).data("key"))
				$form.submit()
			}).on("click", ".date-range li a", function (e) {
				// 选择时间范围
				var me = $(this),
					dateRange = me.data("key")
				// 自定义查询时间范围
				if (dateRange == '${Searchable.QUERY_DATERANGE_CUSTOM}') {
					$(this).daterangepicker({
						opens: "left",
						drops: "up",
						locale: _daterangepickerOptions.localeDate
					}).on("apply.daterangepicker", function (evt, picker) {
						me.closest(".date-range").find(".keyboard").val(dateRange)
						var ranger = $(this).closest(".date-range")
						ranger.find("#startDate").val(picker.startDate.format('YYYY-MM-DD'))
						ranger.find("#endDate").val(picker.endDate.format('YYYY-MM-DD'))
						$form.submit()
					}).data("daterangepicker").show()
				}
				// 内置查询时间范围
				else {
					me.closest(".date-range").find(".keyboard").val(dateRange)
					$form.submit()
				}
			});

			/**
			 * 按输入框关键字查找，去掉右侧分类搜索条件
			 */
			function keyboardRequest() {
				$(".selected :hidden").remove()
				$form.submit()
			}
		})
	</script>
</head>

<body>
<g:form action="index" method="get"><div class="body">
	<section class="content-header">
		<ol class="breadcrumb">
			<li><i class="fa fa-angle-double-right"></i>${entityName}</li>
		</ol>
	</section>

	<div class="box">
		<div class="toolbar">
			<div class="col-sm-12">
				<label>搜索：</label>
				<input type="text" name="q" class="o-search-field" property="code" value="${params.q}">
				<g:if test="${mainCategories}">
					<g:select name="category" from="${mainCategories}" noSelection="['': '--']"
							  value="${params.category}" optionKey="id" optionValue="name"/>
				</g:if>
				<button type="submit" class="btn o-search-submit "><i class="fa fa-search"></i></button>
			</div>
		</div>

		<div class="box-body">
			<div class="row">
				<div class="col-md-9">
				%{--<div class="chunk">
					<h3><a href="javascript:void(0);"><em>张三</em>安居客和低价的话说离开大放送麦克风呢空间撒谎的</a></h3>
					<p class="content-body">简介: 一个可以让人变为任何奥特曼，名为“奥特曼任意键”的系统降临，让平凡少年的命运齿轮为之错开。在黑暗的都市中，与怪兽的战斗中，与其他奥特曼的交流中，感悟...</p>
					<p class="content-footer">类型：类一&nbsp;&nbsp;&nbsp;&nbsp;创建日期：2016-11-25</p>
				</div>--}%
					<g:each in="${list}" var="m">
						<div class="chunk">
							<% String body = m[Constants.FIELD_SUMMARY] %>
							<h3><a target="_blank"
								   href="${applicationUrls[m[Constants.FIELD_APPLICATION]?.toLong()] + m[Constants.FIELD_URL]}">
								${m[Constants.FIELD_DOCNUMBER]} ${m[Constants.FIELD_TITLE]}</a></h3>
							<g:if test="${body}"><p class="content-body">
								${StringUtils.truncate(body, 200, "...")}</p>
							</g:if>
							<p class="content-footer">
								起草日期：
								<g:if test="${m[Constants.FIELD_DATECREATED]}">
									${DateUtils.formatDatetime(DateUtils.parse(m[Constants.FIELD_DATECREATED]))}
								</g:if>
								<g:if test="${m[Constants.FIELD_CREATEDBY]}">
									&nbsp;&nbsp;起草人：
									${m[Constants.FIELD_CREATEDBY]}
								</g:if>
							</p>
						</div>
					</g:each>
					<g:if test="${!list}">
						<div style="text-align: center; padding-top: 1em;">没找到您想要的数据 :(</div>
					</g:if>
				</div>

				<div class="col-md-3">
					<g:if test="${(params.cg && (params.cg.values() - "")) || params.type || params.dateRange}">
						<div class="box box-default box-solid selected">
							<div class="box-header with-border">
								<h3 class="box-title">已选择</h3>

								<div class="box-tools pull-right">
									<button type="button" class="btn btn-box-tool" data-widget="collapse">
										<i class="fa fa-minus"></i></button>
									<button type="button" class="btn btn-box-tool" data-widget="remove">
										<i class="fa fa-times"></i></button>
								</div>
							</div>

							<div class="box-body">
								<ul class="nav nav-stacked">
									<g:each in="${params.cg}" var="category">
										<g:if test="${category.value}">
											<li>
												<g:hiddenField name="cg.${category.key}" value="${category.value}"/>
												<a href="javascript:void(0);">
													<i class="fa fa-fw fa-stumbleupon"></i>${category.value}
													<span class="pull-right"><i class="fa fa-times"></i></span>
												</a>
											</li>
										</g:if>
									</g:each>
									<g:if test="${params.type}">
										<li>
											<g:hiddenField name="type" value="${params.type}"/>
											<a href="javascript:void(0);">
												<i class="fa fa-fw fa-stumbleupon"></i>
												<g:message code="bropen.search.config.Searchable.type.${params.type}"/>
												<span class="pull-right"><i class="fa fa-times"></i></span>
											</a>
										</li>
									</g:if>
									<g:if test="${params.dateRange}">
										<li>
											<g:hiddenField name="dateRange" value="${params.dateRange}"/>
											<g:hiddenField name="startDate" value="${params.startDate}"/>
											<g:hiddenField name="endDate" value="${params.endDate}"/>
											<a href="javascript:void(0);">
												<i class="fa fa-fw fa-stumbleupon"></i>
												<g:if test="${params.dateRange != Searchable.QUERY_DATERANGE_CUSTOM.toString()}">
													<g:message
															code="bropen.search.config.Searchable.dateRange.${params.dateRange}"/>
												</g:if><g:else>
													${params.startDate} 至 ${params.endDate}
												</g:else>
												<span class="pull-right"><i class="fa fa-times"></i></span>
											</a>
										</li>
									</g:if>
								</ul>
							</div>
						</div>
					</g:if>

					<g:if test="${!params.type}">
						<div class="box box-default box-solid type">
							<div class="box-header with-border">
								<g:hiddenField class="keyboard" name="type"/>
								<h3 class="box-title">结果类型</h3>

								<div class="box-tools pull-right">
									<button type="button" class="btn btn-box-tool" data-widget="collapse">
										<i class="fa fa-minus"></i></button>
								</div>
							</div>

							<div class="box-body">
								<ul class="nav nav-stacked">
									<li><a href="javascript:void(0);" data-key="${Searchable.QUERY_DOC_CONTENT}">
										<i class="fa fa-fw fa-stumbleupon"></i>
										<g:message
												code="bropen.search.config.Searchable.type.${Searchable.QUERY_DOC_CONTENT}"/>
									</a></li>
									<li><a href="javascript:void(0);" data-key="${Searchable.QUERY_DOC_ATTACHMENT}">
										<i class="fa fa-fw fa-stumbleupon"></i>
										<g:message
												code="bropen.search.config.Searchable.type.${Searchable.QUERY_DOC_ATTACHMENT}"/>
									</a></li>
								</ul>
							</div>
						</div>
					</g:if>

					<g:each in="${categories}" var="category">
						<g:if test="${category && category.value.size() > 1}">
							<div class="box box-default box-solid category">
								<div class="box-header with-border">
									<g:hiddenField class="keyboard" name="cg.${category.key}"/>
									<h3 class="box-title">${DataDictData.get(category.key.substring(1)).val}</h3>

									<div class="box-tools pull-right">
										<button type="button" class="btn btn-box-tool" data-widget="collapse">
											<i class="fa fa-minus"></i></button>
									</div>
								</div>

								<div class="box-body">
									<ul class="nav nav-stacked">
										<g:each in="${category.value}" var="c">
											<li><a href="javascript:void(0);" data-key="${c.key}">
												<i class="fa fa-fw fa-stumbleupon"></i>${c.key}
											</a></li>
										</g:each>
									</ul>
								</div>
							</div>
						</g:if>
					</g:each>

					<g:if test="${!params.dateRange}">
						<div class="box box-default box-solid date-range">
							<div class="box-header with-border">
								<g:hiddenField class="keyboard" name="dateRange"/>
								<g:hiddenField class="keyboard" name="startDate"/>
								<g:hiddenField class="keyboard" name="endDate"/>
								<h3 class="box-title">更新时间选择</h3>

								<div class="box-tools pull-right">
									<button type="button" class="btn btn-box-tool" data-widget="collapse">
										<i class="fa fa-minus"></i></button>
								</div>
							</div>

							<div class="box-body">
								<ul class="nav nav-stacked">
									<% List queryDateRanges = [Searchable.QUERY_DATERANGE_TWOWEEKS,
															   Searchable.QUERY_DATERANGE_THREEMONTHS,
															   Searchable.QUERY_DATERANGE_THISYEAR,
															   Searchable.QUERY_DATERANGE_LASTYEARS,
															   Searchable.QUERY_DATERANGE_CUSTOM] %>
									<g:each in="${queryDateRanges}" var="queryDateRange">
										<li><a href="javascript:void(0);" data-key="${queryDateRange}">
											<i class="fa fa-fw fa-stumbleupon"></i>
											<g:message
													code="bropen.search.config.Searchable.dateRange.${queryDateRange}"/>
										</a></li>
									</g:each>
								</ul>
							</div>
						</div>
					</g:if>
				</div>
			</div>
		</div>
		<g:if test="${total < params.max}">
			<div class="pagination">
				<o:paginate total="${total}"/>
			</div>
		</g:if>
	</div>
</div></g:form>
</body>
</html>
