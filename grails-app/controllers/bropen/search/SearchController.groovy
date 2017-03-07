package bropen.search

import bropen.framework.core.security.DomainApplication
import bropen.search.config.Searchable
import grails.transaction.Transactional

@groovy.transform.CompileStatic
@Transactional(readOnly = true)
class SearchController {

	static public String VIEW_PATH = "/bropen/search/"

	SearchService searchService
	static private Map<Long, String> applicationUrls = null


	def index() {
		initApplicationUrls()
		Long userId = (Long) session.getAttribute(bropen.framework.Constants.SS_USER_ID)
		List<Map> mainCategories = Searchable.getAllCategories([main: true])
		if (params.q) {
			Map result = searchService.search(userId, (Map) params)
			result.mainCategories = mainCategories
			result.applicationUrls = applicationUrls
			render(view: "${VIEW_PATH}index", model: result)
		} else {
			render(view: "${VIEW_PATH}index", model: [mainCategories: mainCategories])
		}
	}

	private void initApplicationUrls() {
		if (applicationUrls == null) {
			synchronized (this.class) {
				if (applicationUrls != null) return
				applicationUrls = [:]
				List<DomainApplication> applications = DomainApplication.findAll([cache: false])
				for (DomainApplication app in applications) {
					String url = app.getServerURL()
					applicationUrls.put(app.id.toLong(), url.substring(0, url.length() - 1))
				}
			}
		}
	}

}