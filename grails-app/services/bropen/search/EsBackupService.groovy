/* Copyright © 2016 北京博瑞开源软件有限公司版权所有。*/
package bropen.search

import bropen.search.es.EsCoreService
import bropen.search.es.EsSearchService

/**
 * ES 索引备份服务
 */
//NOTFIX @黄超：需如何重构？下面是不是直接保存成 json 比较合适，分拆成多个文件、然后打包也行
@Deprecated
class EsBackupService {

	EsCoreService esCoreService
	EsSearchService esSearchService

	/**
	 * 功能：导出es服务器上所有的索引中的数据到本地
	 * (注：需重构，暂未使用)
	 * @param index
	 * @param type
	 */
	public void backup(File file, Map options = null) {
		/*QueryBuilder query = QueryBuilders.matchAllQuery()
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))

		Integer max = esCoreService.getImportPageSize(), page = 1
		Page result = null
		while (!result || result.hasMore()) {
			result = esSearchService.search([query: query, page: page ++, max: max, index: options.index, type: options.type])
			for (SearchHit hit in result.hits) {
				pw.print(hit.getId())
				pw.print("-")
				pw.print(hit.field("_parent")?.value().toString() ?: "")
				pw.println(hit.getSourceAsString())
			}
			pw.flush()
		}
		pw.close()*/
	}

	/**
	 * 功能：批量导入
	 * (注：需重构，暂未使用)
	 * @param file filename格式为index-type.txt
	 */
	public void restore(File file) {
		/*log.debug("----esToolService------bulkImport-------L1006----file:${file.getAbsolutePath()}")
		Client client = esCoreService.getClient()

		BulkRequestBuilder bulkRequest = client.prepareBulk()
		String index = file.getName().split("-")[0]
		String type = file.getName().split("-")[1].replaceAll("(\\..+)?", "")
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
		String line = null
		int count = esCoreService.getImportPageSize(), i = 0
		while ((line = br.readLine()) != null) {
			int xiabiao = line.indexOf("{")
			String[] idParentId = line.substring(0, xiabiao).split("-")
			if (idParentId.length == 1) {
				bulkRequest.add(
						client.prepareIndex(index, type, idParentId[0])
								.setSource(line.substring(xiabiao)))
			} else {
				bulkRequest.add(
						client.prepareIndex(index, type, idParentId[0])
								.setParent(idParentId[1])
								.setSource(line.substring(xiabiao)))
			}
			if (i % count == 0) {
				bulkRequest.execute().actionGet()
			}
			i++
		}
		bulkRequest.execute().actionGet()
		br.close()*/
	}

}