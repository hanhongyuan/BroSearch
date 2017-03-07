ES文档资源：
	https://www.elastic.co/guide/index.html
	https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
		http://es.xiaoleilu.com/ -- 《Elasticsearch 权威指南（中文版）》
	https://www.elastic.co/guide/en/elasticsearch/guide/current/index.html
	https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-api.html
	https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/index.html

公司文档镜像：
	http://service.bropen.com.cn/tech_docs/docs/elastic/
	http://service.bropen.com.cn/tech_docs/docs/elastic/en/elasticsearch/client/java-api/current/index.html

========================================================================================================================

安装说明

=======================================

公司测试服务器：
	192.168.0.31 Ubuntu 16.04
	elastic/password
	/opt/elasticsearch-5.2.1

安装 JDK8
1、Ubuntu 16.04
	# vi /etc/apt/resource.list
		将所有地址改成 http://mirrors.aliyun.com/ubuntu
	# apt update; apt -y upgrade; apt -y install default-jdk
	# dpkg-reconfigure locales
		调整默认 locale 为 zh_CN.UTF-8
	# dpkg-reconfigure tzdata
		调整时区为 Asia/Shanghai
2、其他OS
	略

系统配置：
1、新增内核参数
	# vi /etc/sysctl.conf
		fs.file-max=65536
		vm.max_map_count=262144
	# sysctl -p
	如果是在 Docker/LXC 的虚机中安装，应调整物理主机的内核参数（调 max_map_count 就够了），并重启虚机。
2、设置资源参数
	# vi /etc/security/limits.conf
		* soft nofile 32768
		* hard nofile 65536
3、修改进程数
	$ ulimit -u 2048
4、创建一个普通用户（es 5不允许使用 root 用户启动），如果自己的开发环境，一般用当前用户即可
	# adduser elastic

安装 ES 5.x：
1、从官网下载 es 后，解压
	https://www.elastic.co/downloads/elasticsearch
2、按需修改配置
	$ cp config/elasticsearch.yml config/elasticsearch.yml.org
	$ vi config/elasticsearch.yml
		# 集群名称
		cluster.name: bropen
		# 节点名称
		node.name: xxxx
		# 设置绑定的ip地址
		network.bind_host: 0.0.0.0
		# 设置其它节点和该节点交互的ip地址
		network.publish_host: 0.0.0.0
		# 用来同时设置bind_host和publish_host参数
		network.host: 0.0.0.0
		# 端口
		transport.tcp.port: 9300
		# for head plugin
		http.cors.enabled: true
		http.cors.allow-origin: "*"
	注意 2.x 里的 script.inline、script.indexed、script.ingest 配置已失效
3、通过 config/jvm.options 来设置 jvm 参数
4、设置为自启动
	# vi /etc/rc.local
		su - elastic "/opt/elasticsearch-5.2.1/bin/elasticsearch" &

安装中文分词插件 IK：
1、从官网下载版本号对应的 zip 包
	https://github.com/medcl/elasticsearch-analysis-ik/releases
	注意不要 clone 命令下载，否则下载的源码还得自己打包
2、解压到 plugins 文件夹下，重启即可

安装Web前端插件 head：
1、方式1，安装基于 nodejs 的 server，详见：
	https://github.com/mobz/elasticsearch-head#running-with-built-in-server
2、方式2，下载 zip 包
	解压在 search 插件中
		src/main/resources/public/developer/elasticsearch-head
	启动工程后，可以直接通过下面的链接访问
		http://localhost:8481/Foobar/developer/elasticsearch-head/index.html
	或者解压在任何地方，使用任何 http server 配置上即可。

启动 ES：
$ bin/elasticsearch

部署附件解析服务器：
1、根据 ~/BroFileparser_4.8.1/docs/readme.md 中的说明，打包成应用
2、解压到服务器上，并修改 application.yml 中的附件仓库路径、端口、IP地址等
3、运行 bin/bropen-fileparser 启动

测试/验证：
1、启动附件解析服务器
	./gradlew bootRun
2、启动 Foobar，配置系统参数并重启
	bropen.search.ElasticSearch.hosts
	bropen.search.url.file.parse
3、在 console 中依次执行下面的脚本，并观察 Foobar、ES、file parse 的日志、访问 head 服务查询索引创建情况
	// 停止计划任务、邮件等
	ctx.quartzScheduler.standby()
	config.grails.mail.disabled = false
	// 索引用户
	ctx.userIndexService.init()
	// 索引文档
	bropen.framework.core.Locker.unlock(bropen.search.Constants.INIT_LOCK, false)
	ctx.searchableIndexService.init()
	// 索引附件
	bropen.framework.core.Locker.unlock(bropen.search.Constants.INIT_LOCK, false)
	ctx.attachmentIndexService.init()
4、用普通用户登录，访问搜索界面，测试数据是否能搜索出来
	http://localhost:8481/Foobar/search

========================================================================================================================

TODO @黄超：建议参考一下下面的东西，进行一些优化、调整，包括 API 啊等等
http://noamt.github.io/elasticsearch-grails-plugin/docs/ref/index.html
https://github.com/gpc/searchable

DONE @黄超：permission 的数据太冗余了，需精简数据内容和数据结构

TODO @黄超
	https://service.bropen.com.cn/jira/browse/BROFWK-555
	https://service.bropen.com.cn/jira/browse/BROFWK-556
	DONE 1d   后台索引
	DONE 0.5d 处理监听器
	DONE 1.0d 处理依赖关系 -- 要拆
		去掉 PROCESS_CLAZZES、TYPE_PROCESSINST
	DONE 0.5d +tasks & search @ processindexservice
	DONE 0.2d 索引URL ---> searchable 加一个 url 属性，格式为 asfdadsf/xxxx/{id}，默认拼 /show/id
    DONE 1.0d Searchable 的 UI：注意索引名称 index 不允许修改；加上 url，也不允许修改
    0.5d 查询结果界面
	0.5d 优化
	0.5d 测试用例
	DONE 0.5d 索引任务，便于搜索待办、待阅 -- 加一个索引来实现（？好像不行，文档没在），类似通讯录
	0.2d Mobile里搜索任务集成

DONE @张文秀：合并到 4.6，需要注意业务流程的索引 -> 业务文档重构

TODO 加 store、index 注解，表示是否要保存、是否要索引：如果不需要显示，则无需 store；如果不需要被搜索，则不需要被 index
TODO 《大数据架构商业之路》：“12.10 业务需求：给点提示吧”

DONE 如何才能不依赖 BPM
NOTFIX 测试 shield 插件
TODO 集成一个调试、监控的前端

DONE 附件解析前，先过滤掉一下常见的不需要解析的文件，比如音视频、压缩包等
DONE client.addTransportAddresses 支持多个地址配置，应用到系统中
TODO 索引初始化后，做一次 Segment merging
TODO 索引初始化时，可考虑 disabling replicas（设置index.number_of_replicas为0），或者关掉一些节点
DONE 如果有线程在 init，则不运行 job 和 数据库监听
DONE 分类/切面
DONE 升级到 ES 5.x

TODO 安全加固
1、 增加验证，官方推荐并且经过认证的是shield插件，该项目为收费项目，可以试用30天。
	网络中也有免费的插件，可以使用 elasticsearch-http-basic，searchguard 插件。
	Shield 可以通过bin/plugin install [github-name]/[repo-name] 形式安装。
2、 使用Nginx搭建反向代理，通过配置Nginx实现对Elasticsearch的认证。
3、 如果是单台部署的Elasticsearch，9200端口不要对外开放。
4、 使用1.7.1以上的版本。在1.7.1以上版本目前还没有爆出过相关漏洞。

========================================================================================================================

ChecEP 工程测试 @ 2016.09.30～10.02
	ES
		1G内存不够，索引 cm.finance.plan.FinancePlan（cm_finance_plan__finance_plan） 时内存几乎溢出
		2G内存时，gc也比较频繁，每次gc时间也比较长
		3G内存时，还是在初始化上面那个索引的时候 gc 频繁、并且耗时较长，72个文档占用1.8G磁盘空间（之后自动优化到164M）、索引大小306M
		大量索引文档、附件时，CPU 明显比较高
		附件索引后，不算 FinancePlan，总共占用磁盘 1.25G（很多附件未成功索引肯定有一定影响）。
	ChecEP
		部署为开发环境，但修改DataSource指向一个临时导入的数据库
		2G内存不够，很快就到顶了
		4G内存勉强，本地总共耗时 26+ 分钟，主要就在上面那个索引；测试机肯定挂掉，跟数据库性能关系比较大：（
		总共 172290 个附件、316G，需要解析的 106716 个，解析成功了 30294 个，不到 1/3，单线程共耗时65分钟
	BroFileParser
		2G内存，足够了，虽然GC也算频繁，但是每次GC都能释放大量内存，未发现溢出现象
		7w多个附件解析失败，很大部分原因是下面这种异常，甚至普通文本文件也会，异常详见 ParseServlet 里的注释：
			parsing: D:\CC\resources\attachment\2015-10\FEEAD5B4-A1B6-493E-AA49-9459B986BEAF
			java.nio.file.NoSuchFileException: D:\CC\Tomcat7_BroFileParser\temp\apache-tika-4193459299938895091.tmp
		而将文件放在本地（Linux）解析完全正常，需分析原因。
	服务器：
		10.102.248.64  -- 测试服务器 Win7
		10.102.248.105 -- 生产服务器，部署 BroFileParser，端口 9080，需配置防火墙打开端口
		tv：629437205/112233，123456
	测试脚本：
		ctx.settingService.createOrUpdate("bropen.search.url.file.parse",
        				"http://127.0.0.1:9080/fileparser/default", null, null, null,
        				[notes: "附件内容解析服务器的地址，含仓库名称"])
        // 停止计划任务、邮件等
		ctx.quartzScheduler.standby()
		config.grails.mail.disabled = false
		// 索引用户
		ctx.userIndexService.init()
		// 索引文档
		bropen.framework.core.Locker.unlock(bropen.search.Constants.INIT_LOCK, false)
		ctx.processIndexService.init()
		// 索引附件
		bropen.framework.core.Locker.unlock(bropen.search.Constants.INIT_LOCK, false)
		ctx.attachmentIndexService.PARSE_HTTP_OPTIONS = [connectionTimeout: 60000, socketTimeout: 60000]
		ctx.attachmentIndexService.PARSE_THREADS_COUNT = 1
		ctx.attachmentIndexService.init()
