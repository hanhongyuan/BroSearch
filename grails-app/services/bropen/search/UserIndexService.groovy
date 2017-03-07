/* Copyright © 2016-2017 北京博瑞开源软件有限公司版权所有。*/
package bropen.search

import bropen.framework.core.osm.Group
import bropen.framework.core.osm.Employee
import bropen.framework.core.osm.EmployeeIdentity
import bropen.framework.core.osm.Organization
import bropen.framework.core.osm.Position
import bropen.framework.core.security.Role
import bropen.framework.core.security.RoleUser
import bropen.framework.core.security.User
import bropen.search.es.EsCoreService
import bropen.search.es.EsIndexService
import bropen.search.es.EsSearchService
import bropen.toolkit.utils.CollectionUtils
import bropen.toolkit.utils.DateUtils
import bropen.toolkit.utils.grails.DbUtils

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

import static bropen.search.Constants.INDEX_USER
import static bropen.search.Constants.TYPE_USER
import static bropen.search.Constants.LASTUPDATED_DOCUMENT

/**
 * 系统用户索引服务
 */
@grails.transaction.Transactional(readOnly = true)
class UserIndexService {

	EsCoreService esCoreService
	EsIndexService esIndexService

	/**
	 * 初始化所有用户
	 */
	void init() {
		deleteIndex()
		update(DateUtils.parse("2010-10-10"))
	}

	/**
	 * 更新用户
	 *
	 * @param lastUpdated 本次需要索引的用户最近更新时间。空则根据上次更新的索引历史自动计算。
	 */
	void update(Date lastUpdated = null) {
		Map options = [lastUpdated: lastUpdated]
		if (!options.lastUpdated) {
			// 从索引里找最近更新时间
			options.lastUpdated = esIndexService.getLastUpdated(INDEX_USER, LASTUPDATED_DOCUMENT)
		}
		indexUsers(options)
	}

	/**
	 * 更新指定用户
	 */
	void update(List<Long> userIds) {
		assert userIds
		indexUsers([entityIds: [(User.class.name): userIds as Set]])
	}

	/**
	 * 更新指定用户
	 */
	void update(Long userId) {
		assert userId
		indexUsers([entityIds: [(User.class.name): [userId] as Set]])
	}

	/**
	 * 删除用户
	 * @param userId
	 */
	void delete(Long userId) {
		esIndexService.deleteDocument(INDEX_USER, TYPE_USER, userId)
	}

	/**
	 * 删除用户索引
	 */
	private void deleteIndex() {
		esIndexService.deleteIndex(INDEX_USER)
	}

	@grails.compiler.GrailsCompileStatic
	private void createIndex() {
		if (!indexCreated) {
			esIndexService.createIndexIfNotExists(INDEX_USER, [defaultMapping: false])
			indexCreated = true
		}
	}
	private static boolean indexCreated = false

	/**
	 * 索引用户信息
	 *
	 * @param options.lastUpdated
	 * @param options.entityIds {@code Map < String , Collection >}
	 * @param options.max
	 * @param options.offset
	 *
	 * @return [id: 用户id, username: 用户名, uuids: [...]]
	 */
	@grails.compiler.GrailsCompileStatic
	private void indexUsers(Map<String, Object> options) {
		createIndex()
		Integer max = options?.max as Integer,
				offset = options?.offset as Integer ?: 0
		if (max == null) max = esCoreService.getImportPageSize()
		Date now = new Date()

		List<Object> queryParams = [Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE] as List<Object>
		String selectHql = """
			select new map(u.id as uid, u.username as username, e.id as eid,
				ei.id as eiid, o.id as oid, p.id as pid, g.id as gid)"""
		// 查询用户身份、权限、机构、群组
		StringBuilder hql = new StringBuilder("""
			from bropen.framework.core.security.User u left outer join u.employee e
				left outer join e.identities ei left outer join ei.organization o
				left outer join ei.positions p left outer join e.groups g
			where u.enabled=? and (e.id is null or e.disabled=?)
				and (ei.id is null or ei.disabled=?) and (o.id is null or o.disabled=?)""")
		// 查询用户角色
		StringBuilder hqlUserRoles = new StringBuilder("""
			select new map(ru.userId as uid, ru.role.id as rid)
			from bropen.framework.core.security.RoleUser ru
			where ru.userId >= 0""")
		calUserLastUpdatedHql(hql, options?.lastUpdated as Date, queryParams)
		calUserInIdsHql(hql, hqlUserRoles, options?.entityIds as Map<String, Collection>)

		Integer total = User.executeQuery(
				"select count(*) " + hql.toString(), queryParams, [cache: false]).first() as Integer

		List<Map> lastUsers = null
		hql.insert(0, selectHql) << " order by u.id asc"
		String hqlUsers = hql.toString()
		while (total > 0 && offset < total) {
			// 查询用户等信息，并且根据 uid 分组
			List<Map> users = User.executeQuery(hqlUsers, queryParams,
					[max: max, offset: offset, cache: false, fetchSize: max]) as List
			if (lastUsers?.size() > 0) users.addAll(0, lastUsers)
			Map<Long, List<Map>> usersMap = CollectionUtils.groupBy(users, "uid")
			// 计算 offset 并减掉最后那个用户的数据（当前页的数据可能不全）
			offset += users.size()
			if (offset < total) {
				lastUsers = usersMap.remove((Long) (users.last().uid))
				offset -= lastUsers.size()
			}
			if (usersMap) {
				// 查询角色信息
				List<Map> userRoles = User.executeQuery(
						hqlUserRoles.toString() + DbUtils.assembleInLisHql("userId", usersMap.keySet(), [prefix: " and "]),
						[cache: false, fetchSize: max]) as List
				// 重构数据结构
				List<Map> result = formatUsers(usersMap, userRoles)
				// 导入
				esIndexService.indexDocuments(INDEX_USER, TYPE_USER, result, [overwrite: true])
			}
		}
		log.info("Indexed [${total}] users" + (options?.lastUpdated ? (" since [" + DateUtils.formatDatetime(options.lastUpdated as Date) + "].") : "."))

		// 保存最后更新时间
		if (options?.lastUpdated && !options?.entityIds && !options?.offset) {
			esIndexService.saveLastUpdated(INDEX_USER, LASTUPDATED_DOCUMENT, now)
		}
	}

	@grails.compiler.GrailsCompileStatic
	private List<Map> formatUsers(Map<Long, List<Map>> usersMap, List<Map> userRoles) {
		Map<Long, List<Map>> userRolesMap = CollectionUtils.groupBy(userRoles, "uid")
		List<Long> allGrantedRoleIds = (userRolesMap.get(RoleUser.UID_ALL_GRANTED)*.rid ?: EMPTY_LIST) as List<Long>
		// 重构数据结构
		List<Map> result = []
		for (Long uid in usersMap.keySet()) {
			List<String> uuids = ["u" + uid]
			List<Map> u = usersMap.get(uid)
			if (u[0].eid != null) {
				uuids << ("e" + u[0].eid)
			}
			for (Long id in (u*.eiid as Set<Long>)) {
				if (id != null) uuids << ("ei" + id)
			}
			for (Long id in (u*.oid as Set<Long>)) {
				if (id != null) uuids << ("o" + id)
			}
			for (Long id in (u*.pid as Set<Long>)) {
				if (id != null) uuids << ("p" + id)
			}
			for (Long id in (u*.gid as Set<Long>)) {
				if (id != null) uuids << ("g" + id)
			}

			List<Long> roleIds = ((userRolesMap.get(uid)*.rid ?: EMPTY_LIST)) as List<Long>
			for (Long id in roleIds) {
				uuids << ("r" + id)
			}
			for (Long id in allGrantedRoleIds) {
				uuids << ("r" + id)
			}

			Map user = [id: uid, username: u[0].username, uuids: uuids]
			result.push(user)
		}
		return result
	}

	@grails.compiler.GrailsCompileStatic
	private void calUserLastUpdatedHql(StringBuilder hql, Date lastUpdated, List queryParams) {
		if (lastUpdated) {
			hql << " and (u.lastUpdated>? or e.lastUpdated>? or g.lastUpdated>? or o.lastUpdated>? or p.lastUpdated>?)"
			queryParams.addAll([lastUpdated, lastUpdated, lastUpdated, lastUpdated, lastUpdated])
		}
	}

	@grails.compiler.GrailsCompileStatic
	private void calUserInIdsHql(StringBuilder hqlUsers, StringBuilder hqlUserRoles, Map<String, Collection> entityIds) {
		if (!entityIds) return
		Collection uIds = entityIds[User.class.name],
				   eIds = entityIds[Employee.class.name],
				   eiIds = entityIds[EmployeeIdentity.class.name],
				   oIds = entityIds[Organization.class.name],
				   pIds = entityIds[Position.class.name],
				   gIds = entityIds[Group.class.name],
				   rIds = entityIds[Role.class.name]
		if (!uIds && !eIds && !eiIds && !oIds && !pIds && !gIds && !rIds) return
		hqlUsers << " and ("
		boolean b = false
		if (uIds) {
			if (b) hqlUsers << " or"
			b = true
			hqlUsers << DbUtils.assembleInLisHql("u.id", uIds)
		}
		if (eIds) {
			if (b) hqlUsers << " or"
			b = true
			hqlUsers << DbUtils.assembleInLisHql("e.id", eIds)
		}
		if (eiIds) {
			if (b) hqlUsers << " or"
			b = true
			hqlUsers << DbUtils.assembleInLisHql("ei.id", eiIds)
		}
		if (oIds) {
			if (b) hqlUsers << " or"
			b = true
			hqlUsers << DbUtils.assembleInLisHql("o.id", oIds)
		}
		if (pIds) {
			if (b) hqlUsers << " or"
			b = true
			hqlUsers << DbUtils.assembleInLisHql("p.id", pIds)
		}
		if (gIds) {
			if (b) hqlUsers << " or"
			b = true
			hqlUsers << DbUtils.assembleInLisHql("g.id", gIds)
		}
		hqlUsers << ")"

		if (uIds || rIds) {
			hqlUserRoles << " and ("
			if (uIds) hqlUserRoles << DbUtils.assembleInLisHql("userId", uIds)
			if (uIds && rIds) hqlUserRoles << " or"
			if (rIds) hqlUserRoles << DbUtils.assembleInLisHql("role.id", rIds)
			hqlUserRoles << ")"
		}
	}

	private static final List EMPTY_LIST = []

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private BlockingQueue<Object> eventQueue = new ArrayBlockingQueue<Object>(10000)
	private static Thread eventThread = null

	@grails.compiler.GrailsCompileStatic
	public void updateObject(Object obj) {
		synchronized (this.class) {
			if (eventThread == null || !eventThread.alive)
				eventThread = startEventListner(eventQueue)
		}
		if (esCoreService.enabled())
			eventQueue.put(obj)
	}

	@grails.compiler.GrailsCompileStatic
	private Thread startEventListner(final BlockingQueue<Object> eventQueue) {
		return DbUtils.startPersistentThread("bro-search-userindex-listner", 1000l, {
			if (esCoreService.enabled() && eventQueue.size() > 0) {
				try {
					List objects = []
					eventQueue.drainTo(objects, esCoreService.getImportPageSize())
					Map<String, Set> entityIds = [:]
					for (Object o in objects) {
						if (!entityIds.containsKey(o.class.name))
							entityIds[o.class.name] = [] as Set
						entityIds[o.class.name] << o.invokeMethod("getId", null)
					}
					indexUsers([entityIds: entityIds] as Map)
				} catch (Exception e) {
					log.error(null, e)
				}
			}
		})
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private test(ctx) {
		UserIndexService s = ctx.userIndexService
		EsIndexService esIndexService = ctx.esIndexService
		EsSearchService esSearchService = ctx.esSearchService

		def index = bropen.search.Constants.INDEX_USER

		s.deleteIndex()
		assert false == esIndexService.isIndexExists(index)

		s.init()
		esIndexService.refresh(index)
		assert esSearchService.search([index: index]).total > 0
		assert esIndexService.getLastUpdated(index, LASTUPDATED_DOCUMENT)

		return "passed!"
	}

}